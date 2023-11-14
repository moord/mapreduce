#include <filesystem>
#include <fstream>
#include <vector>
#include <list>
#include <functional>
#include <thread>

struct Block {
    size_t from;
    size_t to;
};

struct KeyValue {
    std::string key;
    int value;

    friend std::ostream& operator<<(std::ostream& os, KeyValue &other){
        os << other.key << " " << other.value;
        return os;
    }

    friend std::istream& operator>>(std::istream& is, KeyValue &other){
       other.key = "";
       other.value = 0;
       is >> other.key >> other.value;
       return is;
    }
};

using mapper_func = void(int, Block &);
using reducer_func = void(int);

class MapReduce {
public:
    MapReduce(int mappers_count_, int reducers_count_): mappers_count(mappers_count_), reducers_count(reducers_count_){};

    void run(const std::filesystem::path& input, const std::filesystem::path& output, bool act_combiner = true) {
        auto blocks = split_file(input, mappers_count);

        if( !std::filesystem::exists(output)){
            std::filesystem::create_directory(output);
        } else{
            for (auto const & entry: std::filesystem::directory_iterator(output)) {
                std::filesystem::remove(entry.path());
            }
        }

        // Создаём mappers_count потоков
        // В каждом потоке читаем свой блок данных
        // Применяем к строкам данных функцию mapper
        // Сортируем результат каждого потока
        // Результат сохраняется в файловую систему (представляем, что это большие данные)
        // Каждый поток сохраняет результат в свой файл (представляем, что потоки выполняются на разных узлах)

        std::vector<std::thread> mapper_pool;
        mapper_pool.reserve(mappers_count);

        for (int i = 0; i < mappers_count; ++i) {
            mapper_pool.emplace_back(mapper, i, std::ref(blocks[i]));
        }

        for( auto & t: mapper_pool){
           if( t.joinable()){
               t.join();
           }
        }

        //  Combiner (semi-reducer))
        if( act_combiner){
            for (int i = 0; i < mappers_count; ++i) {
                combiner(output, i);
            }
        }

        // Из mappers_count файлов читаем данные (результат фазы map) и перекладываем в reducers_count (вход фазы reduce)
        // Перекладываем так, чтобы:
        //     * данные были отсортированы
        //     * одинаковые ключи оказывались в одном файле, чтобы одинаковые ключи попали на один редьюсер
        //     * файлы примерно одинакового размера, чтобы редьюсеры были загружены примерно равномерно

        shuffle(output);

        // Создаём reducers_count потоков
        // В каждом потоке читаем свой файл (выход предыдущей фазы)
        // Применяем к строкам функцию reducer
        // Результат сохраняется в файловую систему

        std::vector<std::thread> reducer_pool;
        reducer_pool.reserve(reducers_count);

        for (int i = 0; i < reducers_count; ++i) {
             reducer_pool.emplace_back(reducer, i);
        }

        for( auto & t: reducer_pool){
           if( t.joinable()){
               t.join();
           }
        }
    }

    template<typename U>
    void set_mapper( U mapper_){
        mapper = mapper_;
    }

    template<typename U>
    void set_reducer( U reducer_){
        reducer = reducer_;
    }
private:
    std::vector<Block> split_file(const std::filesystem::path& file, int blocks_count) {
        std::vector<Block>blocks;

        auto filesize = std::filesystem::file_size(file);

        if (filesize > 0) {
            std::ifstream in(file.c_str(), std::ios::binary);

            Block block{0, 0};

            for (auto n = 0; n < mappers_count; n++) {
                block.from = block.to;
                block.to = (filesize / mappers_count) * (n + 1);

                in.seekg(block.to);

                while (in.get() != '\n' && !in.eof());

                block.to = in.tellg();
                blocks.push_back(block);
            }

            in.close();
        }
        return blocks;
    }

    void shuffle(const std::filesystem::path& output){
        std::vector<std::ifstream>in;
        std::ofstream out;

        // расчет примерного размера выходных файлов для фазы reduce
        size_t total_size = 0;

        for (auto const & entry: std::filesystem::directory_iterator(output)) {
           total_size += std::filesystem::file_size(entry.path());
        }

        int r_file_size = total_size / reducers_count; // reducer file size

        //  Многопутевое слияние
        // последовательно считываем по одному значению из всех исходных файлов, находим минимальное, записываем в выходной, считываем новое значение взмен сохраненного и т.д,
        int in_index = 0;
        std::generate_n(std::back_inserter(in), mappers_count, [&]()
                                               {return std::ifstream(output/std::string("map_" + std::to_string(in_index++) + ".txt"));});

        std::vector<KeyValue>kvs;

        KeyValue kv;
        for (auto& f : in) {
            f >> kv;
            kvs.emplace_back(kv);
        }

        std::string prev_key = "";

        int out_index = 0;
        out.open(output/std::string("reduce_" + std::to_string(out_index++) + ".txt"));

        while (1) {
            auto it = std::min_element(std::begin(kvs),
                                       std::end(kvs),
                                       [](const KeyValue & a, const KeyValue & b) {
                                            if (a.key.length() == 0) {return false;}
                                            else if (b.key.length() == 0) {return true;}
                                            else {return (a.key < b.key);}});

            // пока не считаны все значения из всех входных файлов
            if (it->key != "") {
                if (out.tellp() > r_file_size && prev_key != it->key) {
                    out.close();
                    out.open(output/std::string("reduce_" + std::to_string(out_index++) + ".txt"));
                }

                out << *it << "\n";
                prev_key = it->key;

                in_index = std::distance(std::begin(kvs), it);  // считываем новое значение из соответствующего файла
                in[in_index] >> kvs[in_index];                   // взамен сохраненного в выходной файл
            }
            else {
                break;
            }
        }

        out.close();

        for (auto& file : in) {
            file.close();
        }
    }

    void combiner( const std::filesystem::path& output, int index){
        // считываем мап файл, суммируем одинаковые префиксы, заменяем мап файл
        std::ifstream in(output/std::string("map_" + std::to_string(index) + ".txt"));

        std::list<KeyValue>kvs;

        std::string prev_key = "";

        KeyValue kv;

        while (!in.eof()) {
            in >> kv;

            if( kv.key == prev_key){
                kvs.back().value++;
            } else{
                if (kv.value > 0){
                    kvs.emplace_back(kv);
                    prev_key = kv.key;
                }
            }
        }
        in.close();

        std::ofstream out(output/std::string("map_" + std::to_string(index) + ".txt"));

        for (auto &kv: kvs) {
            out << kv << "\n";
        }

        out.close();
    }

    int mappers_count;
    int reducers_count;

    std::function<mapper_func> mapper;
    std::function<reducer_func> reducer;
};

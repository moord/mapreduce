#include <iostream>
#include "mapreduce.h"
#include <filesystem>
#include <iterator>
#include <list>
#include <set>
#include <atomic>


namespace fs = std::filesystem;

int main(int argc, char* argv[]) {

    if (argc != 4)
    {
        std::cerr << "Usage: mapreduce <src> <mnum> <rnum>\n";
        return 1;
    }

    fs::path input(argv[1]);
    fs::path output("./out/");

    int mappers_count = std::atoi(argv[2]);
    int reducers_count = std::atoi(argv[3]);

    try {
        MapReduce mr(mappers_count, reducers_count);

        std::atomic<bool> result;

        for (int n = 1; n < 50; ++n) {
            mr.set_mapper([&](int index, Block & block){
                 std::vector<std::string> map_data;

                 // загружаем
                 std::ifstream in(input.c_str(), std::ios::binary);
                 in.seekg(block.from);

                 char data[100];

                 while( in.tellg() < block.to){
                     in.getline(data,100);
                     data[n] = '\0';
                     std::transform(data, data+n-1, data, [](unsigned char c){ return std::tolower(c); });
                     map_data.emplace_back(data);
                 }

                 std::sort(map_data.begin(), map_data.end());

                 in.close();

                 // сохраняем
                 std::ofstream out(output/std::string("map_" + std::to_string(index) + ".txt"));

                 std::copy(map_data.begin(), map_data.end(), std::ostream_iterator<std::string>(out, " 1\n"));

                 out.close();
            });

            mr.set_reducer([&](int index){
                if( result ){
                    std::ifstream in(output/std::string("reduce_" + std::to_string(index) + ".txt"));

                    std::string prev_prefix = "";

                    KeyValue kv;

                    while (!in.eof()) {
                        in >> kv;

                        if( kv.key == prev_prefix || kv.value > 1){
                            result = false;                             // для текущей длины префикса найден повтор 
                            break;                                      // дальнейший поиск не имеет смысла
                        }
                        prev_prefix = kv.key;
                    }
                    in.close();
                }
            });

            result = true;

            mr.run(input, output);

            if( result ){
                std::cout << "min prefix: " << n << "\n";
                break;
            }
        }

        if( !result){
            std::cout << "min prefix: fail\n";
        }

    }
    catch (fs::filesystem_error& e) {
        std::cerr << e.what() << '\n';
    }

    return 0;
}

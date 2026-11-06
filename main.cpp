// gzip is implemented in zlib library which provides full 
// support for creating and reading standard .gz files

// For this implementation I have chosen a basic worker thread to do async
// compression and decompression without locking the main application thread

// Rough outline:
// Producer --> Raw Data --> Thread-safe Queue --> Process via z_stream -->
// Compression Worker --> Compresed Output --> Write

#include <iostream>
#include <vector>
#include <string>
#include <future>
#include <memory>
#include <stdexcept>
#include <cstring>
#include <zlib.h>

class GzipCompressor {
private:
    z_stream stream_;
    bool initialized_ = false;
    static constexpr size_t ChunkSize = 16384; // 16 KB

public:
    GzipCompressor() {
        std::memset(&stream_, 0, sizeof(stream_));
        // windowBit of 15 + 16 to use strict header format
        // int deflateInit2(z_streamp strm, int level, int method, int windowBits, int memLevel, int strategy);
        // strm - Pointer to a z_stream structure. Must be initialized (e.g., zeroed out) before calling.
        // level - Compression level. Balances speed vs. ratio.
        // method - Compression algorithm.
        // windowBits - Base 2 logarithm of the window size (history buffer) or format modifier.
        // memLevel - Amount of memory to allocate for the internal state (speed vs. memory footprint). 8 (default)
        // strategy - Tuned for specific types of input data.
        int result = deflateInit2(&stream_, Z_DEFAULT_COMPRESSION, Z_DEFLATED, 15+16, 8, Z_DEFAULT_STRATEGY);

        if (result != Z_OK) {
            throw std::runtime_error("Failed to initialize zlib deflate stream");
        }
        initialized_ = true;
    }

    ~GzipCompressor() {
        if (initialized_) {
            deflateEnd(&stream_);
        }
    }

    std::vector<uint8_t> process_chunk(const std::vector<uint8_t>& input_chunk, bool finish = false) {
        std::vector<uint8_t> output_buffer;
        std::vector<uint8_t> temp_out(ChunkSize);

        stream_.next_in = const_cast<Bytef*>(input_chunk.data());
        stream_.avail_in = static_cast<uInt>(input_chunk.size());

        int flush_mode = finish ? Z_FINISH : Z_NO_FLUSH;
        int status = Z_OK;

        while (stream_.avail_in > 0 || flush_mode == Z_FINISH) {
            stream_.next_out = temp_out.data();
            stream_.avail_out = static_case<uInt>(temp_out.size());
            status = deflat(&stream_, flush_mode);

            size_t bytes_produced = ChunkSize - stream_.avail_out;
            if (bytes_produced > 0) {
                output_buffer.insert(output_buffer.end(), temp_out.begin(), temp_out.begin() + bytes_produced);
            }
            if (status == Z_STREAM_END) {
                break;
            }
            if (status != Z_OK && status != Z_BUF_ERROR) {
                throw std::runtime_error("Compression error occured during streaming execution");
            }
            if (stream_.avail_out > 0  && flush_mode != Z_FINISH) {
                break;
            }
        }
        return output_buffer;
    }
};

int main() {
    std::cout <<"Starting Async mode of Gzip streaming pipeline" << std::endl;

    std::vector<std::vector<uint8_t>>incoming_stream = {
{'H','e','l','l','o', ' '},
{'W','o','r','l','d','!',' '},
{'S','t','r','e','a','m','i','n','g', ' '},
{'C,'+','+',' ','D','a','.'}
};

    auto async_future = std::async(std::launch::async, [incoming_stream]() {
        GzipCompressor compressor;
        std::vector<uint8_t> compressed_output;

        for (size_t i = 0; i < incoming_stream.size(); ++i) {
            bool is_last = (i == incoming_stream.size() - 1);
            auto output_chunk = compressor.process_chunk(incoming_stream[i], is_last);
            compressed_output.insert(compressed_output.end(), output_chunk.begin(),output_chunk.end());
            std::count << "Worker Thread PRocessed chunk " << i + 1 << " (" << incoming_stream[i].size() << "raw bytes passed)" <<std::endl;
        }
    return compressed_output;
    });
    std::cout << "Main thread doing concurrent non-blocking tasks" << std::endl;

    std::vector<uint8_t> final_gzip_payload = async_future.get();
    std::cout << "Main Thread Complete. Total Gzip package size: "  << final_gzip_payload.size() << " bytes." << std:endl;
    return 0;
}


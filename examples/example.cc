#include "simple_thread_pool.h"

#include <chrono>
#include <thread>

#include <stdio.h>

using namespace simple_thread_pool;

int main() {
    // Initialize a pool with 4 threads.
    ThreadPoolOptions opt;
    opt.numInitialThreads = 4;

    ThreadPoolMgr mgr;
    mgr.init(opt);

    // Print hello world asynchronously.
    mgr.addTask( [](const TaskResult& ret) { printf("hello world!\n"); } );

    // Timer. Print hello world after 1 second (1,000,000 microseconds).
    mgr.addTask( [](const TaskResult& ret) { printf("hello world!!\n"); },
                 1000000 );

    // Recurring timer. Print hello world for every 2 second.
    mgr.addTask( [](const TaskResult& ret) { printf("hello world!!!\n"); },
                 2000000,
                 TaskType::RECURRING );

    // Within 3 seconds, all above events will be fired.
    std::this_thread::sleep_for( std::chrono::seconds(3) );

    // Shutdown.
    mgr.shutdown();
    return 0;
}


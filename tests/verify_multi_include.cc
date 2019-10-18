#include "simple_thread_pool.h"

void dummy_function() {
    // Do nothing (to check if two different files that include
    // the same header can be linked well without any conflicts).
    simple_thread_pool::ThreadPoolMgr mgr;
}


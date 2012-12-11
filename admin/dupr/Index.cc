#include <stdio.h>
#include <algorithm>
#include <iostream>

#include "boost/scoped_ptr.hpp"
#include "boost/timer.hpp"

#include "Block.h"
#include "Htm.h"
#include "Merger.h"
#include "Options.h"
#include "ThreadUtils.h"

using std::cout;
using std::endl;
using std::max;

using namespace dupr;


namespace {

struct State {
    State(Options const & options, InputBlockVector const & blocks);
    ~State();

    char             cl0[CACHE_LINE_SIZE];

    Mutex            mutex;
    Options const &  options;  // Indexing options
    InputBlockVector blocks;   // Input blocks

    Merger           merger;   // Block merger

    PopulationMap    map;      // Population map

    char             cl1[CACHE_LINE_SIZE];
};

State::State(Options const & options, InputBlockVector const & blocks) :
    mutex(),
    options(options),
    blocks(blocks),
    merger(options.indexDir + "/data.csv",
           options.indexDir + "/ids.bin",
           options.scratchDir + "/scratch.bin",
           options.blockSize,
           options.k,
           blocks.size()),
    map(options.htmLevel)
{ }

State::~State() { }

/* The processing loop for threads. Note that this scheme can be improved on.
   In particular, it would be better to adjust the number of threads that are
   reading blocks separately from the number of blocks that are processing
   blocks. As it stands, saturating IO/CPU will result in over/under
   subscription of CPU/IO, unless the IO rate closely matches the processing
   rate.
 */
void * run(void * arg) {
    State & state = *static_cast<State *>(arg);
    while (true) {
        boost::shared_ptr<InputBlock> block;
        // get a block to process
        {
            Lock lock(state.mutex);
            if (state.blocks.empty()) {
                break; // none left
            }
            block = state.blocks.back();
            state.blocks.pop_back();
        }
        // read the block
        block->read();
        // process the block
        block->process(state.options, state.map);
        // add the block to the merge queue
        state.merger.add(block);
    }
    return 0;
}


void index(Options const & options) {
    int const numThreads = max(1, options.numThreads);
    cout << "Initializing... " << endl;
    boost::timer t;
    boost::scoped_ptr<State> state(new State(
        options, splitInputs(options.inputFiles, options.blockSize)));
    cout << "\tsplit inputs into " << state->blocks.size()
         << " blocks in " << t.elapsed() << " sec" << endl;
    cout << "Indexing input... " << endl;
    t.restart();
    // create thread pool
    boost::scoped_array<pthread_t> threads(new pthread_t[numThreads - 1]);
    for (int t = 1; t < numThreads; ++t) {
        if (::pthread_create(&threads[t-1], 0, &run, static_cast<void *>(state.get())) != 0) {
            perror("pthread_create() failed");
            exit(EXIT_FAILURE);
        }
    }
    // the calling thread participates in processing
    run(static_cast<void *>(state.get()));
    // wait for all threads to finish
    for (int t = 1; t < numThreads; ++t) {
        ::pthread_join(threads[t-1], 0);
    }
    cout << "\tfirst pass finished in " << t.elapsed() << " sec" << endl;
    t.restart();
    // Finish up the merge
    state->merger.finish();
    cout << "\tmerging finished in " << t.elapsed() << " sec" << endl;
    // Write the population map
    state->map.makeQueryable();
    state->map.write(options.indexDir + "/map.bin");
}

} // unnamed namespace


int main(int argc, char ** argv) {
    boost::timer total;
    Options options = parseIndexerCommandLine(argc, argv);
    index(options);
    cout << endl << "Indexer finished in " << total.elapsed() << " sec" << endl;
    return EXIT_SUCCESS;
}


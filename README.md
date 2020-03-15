## MPI_blockchain

### Code
**Main:** `blockchain.cpp`

**Bulk of the algorithm:** `node.cpp`

### Compile:

`make`

### Run main with twenty nodes:
`mpiexec -np 20 ./blockchain`

### Performance benchmarks
`MPI_blockchain/data/plot_time_performance.ipynb`
Note that the more nodes, more chances that at least one of them will get the right answer.

#include "block.h"
#include "node.h"
#include <mpi.h>
#include <iostream>

using namespace std;

// Variables de MPI
MPI_Datatype *MPI_BLOCK;

int main(int argc, char **argv) {

  // Inicializo MPI
  int provided;
  int status= MPI_Init_thread( &argc, &argv, MPI_THREAD_MULTIPLE, &provided );
  if (status != MPI_SUCCESS){
	  fprintf(stderr, "Error de MPI al inicializar.\n");
	  MPI_Abort(MPI_COMM_WORLD, status);
  }

  //Defino un nuevo tipo de datos de MPI para Block: MPI_BLOCK
  MPI_BLOCK = new MPI_Datatype;
  define_block_data_type_for_MPI(MPI_BLOCK);


  // Control del buffering: sin buffering
  setbuf(stdout, NULL);
  setbuf(stderr, NULL);


  system("rm *.out");
  
  //Llama a la función que maneja cada nodo
  node();

  cout << "SALIERON" << endl;

  // Limpio MPI
  MPI_Finalize();
  delete MPI_BLOCK;

  return 0;
}

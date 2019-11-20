#include "node.h"
#include "picosha2.h"
#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include <cstdlib>
#include <queue>
#include <atomic>
#include <mpi.h>
#include <map>

int total_nodes, created_nodes, mpi_rank;
Block *last_block_in_chain;
map<string,Block> node_blocks;

//Cuando me llega una cadena adelantada, y tengo que pedir los nodos que me faltan
//Si nos separan más de VALIDATION_BLOCKS bloques de distancia entre las cadenas, se descarta por seguridad
bool verificar_y_migrar_cadena(const Block *rBlock, const MPI_Status *status){

  //Enviar mensaje TAG_CHAIN_HASH
  MPI_Send(rBlock, 1, *MPI_BLOCK, rBlock->node_owner_number, TAG_CHAIN_HASH, MPI_COMM_WORLD);

  Block *blockchain = new Block[VALIDATION_BLOCKS];

  //Recibir mensaje TAG_CHAIN_RESPONSE
  MPI_Status statusRes;
  MPI_Recv(blockchain, VALIDATION_BLOCKS, *MPI_BLOCK, rBlock->node_owner_number, TAG_CHAIN_RESPONSE, MPI_COMM_WORLD, &statusRes);
  //TODO: Verificar que los bloques recibidos
  //sean válidos y se puedan acoplar a la cadena
    //delete []blockchain;
    //return true;


  delete []blockchain;
  return false;
}

//Verifica que el bloque tenga que ser incluido en la cadena, y lo agrega si corresponde
bool validate_block_for_chain(const Block *rBlock, const MPI_Status *status){
  if(valid_new_block(rBlock)){

    //Agrego el bloque al diccionario, aunque no
    //necesariamente eso lo agrega a la cadena
    node_blocks[string(rBlock->block_hash)]=*rBlock;
    Block blockToSet=*rBlock;
    //Si el índice del bloque recibido es 1
    //y mí último bloque actual tiene índice 0,
    //entonces lo agrego como nuevo último.
    if((rBlock->index==1) && (last_block_in_chain->index==0)){
      last_block_in_chain=&blockToSet;
      printf("[%d] Agregado a la lista bloque con index %d enviado por %d \n", mpi_rank, rBlock->index,status->MPI_SOURCE);
      return true;
    }

    //Si el índice del bloque recibido es
    //el siguiente a mí último bloque actual,
    //y el bloque anterior apuntado por el recibido es mí último actual,
    //entonces lo agrego como nuevo último.
    if((rBlock->index==(last_block_in_chain->index)+1) && (rBlock->previous_block_hash==last_block_in_chain->block_hash)){
      last_block_in_chain=&blockToSet;
      printf("[%d] Agregado a la lista bloque con index %d enviado por %d \n", mpi_rank, rBlock->index,status->MPI_SOURCE);
      return true;
    }

    //Si el índice del bloque recibido es
    //el siguiente a mí último bloque actual,
    //pero el bloque anterior apuntado por el recibido no es mí último actual,
    //entonces hay una blockchain más larga que la mía.
    if((rBlock->index==(last_block_in_chain->index)+1) && (rBlock->previous_block_hash!=last_block_in_chain->block_hash)){
      printf("[%d] Perdí la carrera por uno (%d) contra %d \n", mpi_rank, rBlock->index, status->MPI_SOURCE);
      return verificar_y_migrar_cadena(rBlock,status);
    }


    //Si el índice del bloque recibido es igua al índice de mi último bloque actual,
    //entonces hay dos posibles forks de la blockchain pero mantengo la mía
    if(rBlock->index==(last_block_in_chain->index)){
      printf("[%d] Conflicto suave: Conflicto de branch (%d) contra %d \n",mpi_rank,rBlock->index,status->MPI_SOURCE);
      return false;
    }

    //Si el índice del bloque recibido es anterior al índice de mi último bloque actual,
    //entonces lo descarto porque asumo que mi cadena es la que está quedando preservada.
    if(rBlock->index<(last_block_in_chain->index)){
      printf("[%d] Conflicto suave: Descarto el bloque (%d vs %d) contra %d \n",mpi_rank,rBlock->index,last_block_in_chain->index, status->MPI_SOURCE);
      return false;
    }

    //Si el índice del bloque recibido está más de una posición adelantada a mi último bloque actual,
    //entonces me conviene abandonar mi blockchain actual
    if(rBlock->index>(last_block_in_chain->index)){
      printf("[%d] Perdí la carrera por varios contra %d \n", mpi_rank, status->MPI_SOURCE);
      bool res = verificar_y_migrar_cadena(rBlock,status);
      return res;
    }
  }

  printf("[%d] Error duro: Descarto el bloque recibido de %d porque no es válido \n",mpi_rank,status->MPI_SOURCE);
  return false;
}



void send_block_to_everyone(const Block *block){
  int new_rank;

  for(int i = 1; i < total_nodes; i++){
  
    new_rank = (mpi_rank + i) % total_nodes;

    int send_return_status = MPI_Send(&block, 1, *MPI_BLOCK, new_rank, TAG_NEW_BLOCK, MPI_COMM_WORLD);

    if(send_return_status != MPI_SUCCESS) {
      printf("[%d] send to node %d failed with error code %d \n",mpi_rank, new_rank, send_return_status);
    }
  }
}

//Proof of work
//TODO: Advertencia: puede tener condiciones de carrera
void* proof_of_work(void *ptr){

    string hash_hex_str;
    Block block;
    unsigned int mined_blocks = 0;
    while(true){

      block = *last_block_in_chain;

      //Preparar nuevo bloque
      block.index += 1;
      block.node_owner_number = mpi_rank;
      block.difficulty = DEFAULT_DIFFICULTY;
      block.created_at = static_cast<unsigned long int> (time(NULL));
      memcpy(block.previous_block_hash,block.block_hash,HASH_SIZE);

      //Agregar un nonce al azar al bloque para intentar resolver el problema
      gen_random_nonce(block.nonce);

      //Hashear el contenido (con el nuevo nonce)
      block_to_hash(&block,hash_hex_str);

      //Contar la cantidad de ceros iniciales (con el nuevo nonce)
      if(solves_problem(hash_hex_str)){

          //Verifico que no haya cambiado mientras calculaba
          if(last_block_in_chain->index < block.index){
            mined_blocks += 1;
            *last_block_in_chain = block;
            strcpy(last_block_in_chain->block_hash, hash_hex_str.c_str());
            node_blocks[hash_hex_str] = *last_block_in_chain;
            // printf("[%d] Agregué un producido con index %d \n",mpi_rank,last_block_in_chain->index);

            //TODO: Mientras comunico, no responder mensajes de nuevos nodos
            send_block_to_everyone(last_block_in_chain);
          }
      }

    }

    return NULL;
}

int send_blockchain(Block &buffer, const MPI_Status *status){

  // asume VALIDATION_BLOCKS > 0
  // Defino la cadena a enviar => Voy a llenarla con:
  // min{blockchain.len, VALIDATION_BLOCKS} bloques hacia atrás desde el bloque recibido en buffer 

  unsigned int rank_of_asking_node = status->MPI_SOURCE; // 

  Block *blockchain = new Block[VALIDATION_BLOCKS];
  for (int i = 0; i < VALIDATION_BLOCKS; ++i){
    blockchain[i] = buffer;
    buffer = node_blocks.at(buffer.previous_block_hash);
    if(buffer.previous_block_hash == 0){
      break;
    }
  }

  int send_return_status = MPI_Send(blockchain, VALIDATION_BLOCKS, *MPI_BLOCK, rank_of_asking_node, TAG_CHAIN_RESPONSE, MPI_COMM_WORLD);
  if(send_return_status != MPI_SUCCESS) {
    printf("[%d] send to node %d failed with error code %d \n",mpi_rank, rank_of_asking_node, send_return_status);
  }

  delete []blockchain;

  return 0;
}

Block* initialize_first_block() {
  Block *b = new Block;

  //Inicializo el primer bloque
  b->index = 0;
  b->node_owner_number = mpi_rank;
  b->difficulty = DEFAULT_DIFFICULTY;
  b->created_at = static_cast<unsigned long int> (time(NULL));
  memset(b->previous_block_hash,0,HASH_SIZE);

  return b;
}

int node(){

  //Tomar valor de mpi_rank y de nodos totales
  MPI_Comm_size(MPI_COMM_WORLD, &total_nodes);
  MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);

  //La semilla de las funciones aleatorias depende del mpi_ranking
  srand(time(NULL) + mpi_rank);
  printf("[MPI] Lanzando proceso %u\n", mpi_rank);

  // TODO(charli): verificar si el barrier es necesario. la idea seria asegurar que se crearon todos los procesos
  // MPI_Barrier(MPI_COMM_WORLD);

  last_block_in_chain = initialize_first_block();

  pthread_t thread;
  //Crear thread para minar
  pthread_create(&thread, NULL, proof_of_work, NULL);
  while(true){

      //Recibir mensajes de otros nodos
      Block buffer;
      MPI_Status status;
      MPI_Recv(&buffer, 1, *MPI_BLOCK, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

      //Si es un mensaje de nuevo bloque, llamar a la función
      if(status.MPI_TAG==TAG_NEW_BLOCK){
        printf("[%u] validate_block_for_chain \n", mpi_rank);
        // validate_block_for_chain con el bloque recibido y el estado de MPI
        validate_block_for_chain(&buffer,&status);
      
      }else if(status.MPI_TAG==TAG_CHAIN_HASH){ //Si es un mensaje de pedido de cadena,
        printf("[%u] TAG_CHAIN_HASH \n", mpi_rank);
        send_blockchain(buffer, &status);

      }else{
        printf("NO RECIBIO NADA\n");
      }
  }

  delete last_block_in_chain; // TODO(charli): verificar si se llega a este delete
  return 0;
}
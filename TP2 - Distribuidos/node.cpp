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
#include <iostream> 

using namespace std;

int total_nodes, created_nodes, mpi_rank;
Block *last_block_in_chain;
map<string,Block> node_blocks;
pthread_mutex_t _sendMutex = PTHREAD_MUTEX_INITIALIZER;

//Cuando me llega una cadena adelantada, y tengo que pedir los nodos que me faltan
//Si nos separan más de VALIDATION_BLOCKS bloques de distancia entre las cadenas, se descarta por seguridad
bool verificar_y_migrar_cadena(const Block *rBlock, const MPI_Status *status){

  //Enviar mensaje TAG_CHAIN_HASH
  MPI_Send(rBlock, 1, *MPI_BLOCK, rBlock->node_owner_number, TAG_CHAIN_HASH, MPI_COMM_WORLD);

  Block *received_blockchain = new Block[VALIDATION_BLOCKS];

  //Recibir mensaje TAG_CHAIN_RESPONSE
  MPI_Status statusRes;
  MPI_Recv(received_blockchain, VALIDATION_BLOCKS, *MPI_BLOCK, rBlock->node_owner_number, TAG_CHAIN_RESPONSE, MPI_COMM_WORLD, &statusRes);

  // recorrer cadena recibida
  bool invalid = false;
  int i;
  // TODO(cgiudice): el ultimo bloque de la lista esta en el cero?
  // El primer bloque de la lista contiene el hash pedido y el mismo index que el bloque original?
  if(rBlock->index == received_blockchain[0].index 
    && string(received_blockchain[0].block_hash).compare(rBlock->block_hash) == 0){
    
    // El hash del bloque recibido es igual al calculado por la función block_to_hash.
    string hash_hex_str;
    block_to_hash(&received_blockchain[0],hash_hex_str);

    if(string(received_blockchain[0].block_hash).compare(hash_hex_str) == 0) {

      
      Block * current_node_from_list = last_block_in_chain;
      for (i = 0; i < VALIDATION_BLOCKS; ++i){

        Block current_received_block = received_blockchain[i];
        if(current_received_block.previous_block_hash == 0){
          break;
        }
        Block previous_block = received_blockchain[i+1];

        // Cada bloque siguiente de la lista, contiene el hash definido en previous_block_hash del actual elemento.
        // Cada bloque siguiente de la lista, contiene el índice anterior al actual elemento.
        if(string(current_received_block.previous_block_hash).compare(previous_block.block_hash) == 0
            && current_received_block.index - 1 == previous_block.index){

          if(current_node_from_list->block_hash == current_received_block.block_hash ) { 
            // found common node 
            break; 
          } else {
            current_node_from_list = &node_blocks.at(current_node_from_list->previous_block_hash);
          }
        } else {
          invalid = true;
        }
      }
    } else {
      invalid = true;
    }
  } else {
    invalid = true;
  }
  if(!invalid) {
    // deleteo lo que tengo que descartar de mi cadena
    Block * current_node_from_list = last_block_in_chain;
    while(current_node_from_list->previous_block_hash != 0){
      // copy hash as key
      string map_entry_to_delete = string(current_node_from_list->block_hash);

      // get next elem
      current_node_from_list = &node_blocks.at(current_node_from_list->previous_block_hash);

      // erase map entry
      node_blocks.erase(map_entry_to_delete);
    }
    // seteo nuevo last elements
    last_block_in_chain = &received_blockchain[0];

    // agrego las entradas de la nueva cadena
    for(int j = 0; j < i; j++){
      Block current_received_block = received_blockchain[j];

      // TODO(charli): asegurar que current_received_block se pasa bien por copia
      node_blocks.insert({string(current_received_block.block_hash), current_received_block}); 

      if(current_received_block.previous_block_hash == 0) break;
    }

    delete []received_blockchain;
    return true;
  }

  delete []received_blockchain;
  return false;
}

//Verifica que el bloque tenga que ser incluido en la cadena, y lo agrega si corresponde
bool validate_block_for_chain(const Block *rBlock, const MPI_Status *status){
  if(valid_new_block(rBlock)){

    //Agrego el bloque al diccionario, aunque no
    //necesariamente eso lo agrega a la cadena
    node_blocks[string(rBlock->block_hash)]=*rBlock;
    
    //Block blockToSet=*rBlock; LO DEJO COMENTADO POR LAS DUDAS :) 
    
    //Si el índice del bloque recibido es 1
    //y mí último bloque actual tiene índice 0,
    //entonces lo agrego como nuevo último.
    if((rBlock->index==1) && (last_block_in_chain->index==0)){
      last_block_in_chain=(Block *) rBlock;
      printf("[%d] Agregado a la lista bloque con index %d enviado por %d \n", mpi_rank, rBlock->index, status->MPI_SOURCE);
      return true;
    }

    //Si el índice del bloque recibido es
    //el siguiente a mí último bloque actual,
    //y el bloque anterior apuntado por el recibido es mí último actual,
    //entonces lo agrego como nuevo último.
    if((rBlock->index==(last_block_in_chain->index)+1) && (rBlock->previous_block_hash==last_block_in_chain->block_hash)){
      last_block_in_chain=(Block *) rBlock;
      printf("[%d] Agregado a la lista bloque con index %d enviado por %d \n", mpi_rank, rBlock->index,status->MPI_SOURCE);
      return true;
    }

    //Si el índice del bloque recibido es
    //el siguiente a mí último bloque actual,
    //pero el bloque anterior apuntado por el recibido no es mí último actual,
    //entonces hay una blockchain más larga que la mía.
    if((rBlock->index==(last_block_in_chain->index)+1) && (rBlock->previous_block_hash!=last_block_in_chain->block_hash)){
      printf("[%d] Perdí la carrera por uno (%d) contra %d \n", mpi_rank, rBlock->index, status->MPI_SOURCE);
      bool res = verificar_y_migrar_cadena(rBlock,status);
      return res;
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

void print_block(const Block *block){
  //cout << "--------------------" << endl;
  //cout << "Block number: " << block->index << endl;
  //cout << "Owner: " << block->node_owner_number << endl;
  //cout << "Difficulty: " << block->difficulty << endl;
  //cout << "Created at: " << block->created_at << endl;
  //cout << "Nonce: " << (string)block->nonce << endl;
  //cout << "Previous block hash: " << (string)block->previous_block_hash << endl;
  //cout << "Block hash: " << (string)block->block_hash << endl;
  //cout << "--------------------" << endl;
}

void send_block_to_everyone(const Block block){
  int new_rank;
  cout << "[" + to_string(mpi_rank) + "]: total nodes " + to_string(total_nodes) + "." << endl;

  for(int i = 1; i < total_nodes; i++){
  
    new_rank = (mpi_rank + i) % total_nodes;

    cout << "[" + to_string(mpi_rank) + "]: sending to " + to_string(new_rank) + "." << endl;

    //print_block(&block);

    int send_return_status = MPI_Send(&block, 1, *MPI_BLOCK, new_rank, TAG_NEW_BLOCK, MPI_COMM_WORLD);

    cout << "[" + to_string(mpi_rank) + "]: sent to " + to_string(new_rank) + "." << endl;

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
      
      ////cout << "[" + to_string(mpi_rank) + "]: memcpy done." << endl;
      
      //Agregar un nonce al azar al bloque para intentar resolver el problema
      gen_random_nonce(block.nonce);

      //Hashear el contenido (con el nuevo nonce)
      block_to_hash(&block,hash_hex_str);

      //Contar la cantidad de ceros iniciales (con el nuevo nonce)
      if(solves_problem(hash_hex_str)){

          //cout << "[" + to_string(mpi_rank) + "]: about to broadcast." << endl;


          //Verifico que no haya cambiado mientras calculaba
          pthread_mutex_lock(&(_sendMutex));

          if(last_block_in_chain->index < block.index){

            //cout << "[" + to_string(mpi_rank) + "]: same length." << endl;

            mined_blocks += 1;
            *last_block_in_chain = block;
            strcpy(last_block_in_chain->block_hash, hash_hex_str.c_str());
            node_blocks.insert({hash_hex_str, *last_block_in_chain}); //[hash_hex_str] = *last_block_in_chain;
            printf("[%d] Agregué un producido con index %d \n",mpi_rank,last_block_in_chain->index);

            //TODO: Mientras comunico, no responder mensajes de nuevos nodos
            //cout << "[" + to_string(mpi_rank) + "]: mutex lock." << endl;
            send_block_to_everyone(*last_block_in_chain);
            //cout << "[" + to_string(mpi_rank) + "]: broadcast done." << endl; 
          }
          pthread_mutex_unlock(&(_sendMutex));

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
    cout << "[" + to_string(mpi_rank) + "]: estoy agregando el bloque " + to_string(i) + " de " + to_string(total_nodes) + " y el anterior tiene hash " + (string)buffer.previous_block_hash << endl;
    if(buffer.previous_block_hash == 0){
      cout << "[" + to_string(mpi_rank) + "]: salgo" << endl;
      break;
    }
    cout << "[" + to_string(mpi_rank) + "]: sigo  " << endl;
    buffer = node_blocks.at(buffer.previous_block_hash);
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
  memset(b->block_hash,0,HASH_SIZE);
  
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

      cout << "[" + to_string(mpi_rank) + "]: tomo mutex." << endl; 

      pthread_mutex_lock(&(_sendMutex));

      cout << "[" + to_string(mpi_rank) + "]: tomé mutex." << endl;

      //Si es un mensaje de nuevo bloque, llamar a la función
      if(status.MPI_TAG==TAG_NEW_BLOCK){
        printf("[%u] validate_block_for_chain \n", mpi_rank);
        // validate_block_for_chain con el bloque recibido y el estado de MPI
        /*printf("[%u] Recibí bloque \n", mpi_rank);
        print_block(&buffer);*/
        validate_block_for_chain(&buffer,&status);
      
      }else if(status.MPI_TAG==TAG_CHAIN_HASH){ //Si es un mensaje de pedido de cadena,
        printf("[%u] TAG_CHAIN_HASH \n", mpi_rank);
        send_blockchain(buffer, &status);

      }else{
        printf("NO RECIBIO NADA\n");
      }

      pthread_mutex_unlock(&(_sendMutex));

      cout << "[" + to_string(mpi_rank) + "]: largué mutex." << endl;

  }

  delete last_block_in_chain; // TODO(charli): verificar si se llega a este delete
  return 0;
}
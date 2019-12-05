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
#include <fstream>
#include <assert.h>     /* assert */
#include <signal.h>


using namespace std;

int total_nodes, created_nodes, mpi_rank;
Block *last_block_in_chain;
map<string,Block> node_blocks;
unsigned int mined_blocks = 0;
unsigned long tid;


pthread_mutex_t _sendMutex = PTHREAD_MUTEX_INITIALIZER;

void print_block(const Block *block){
  cout << "--------------------" << endl;
  cout << "Block number: " << block->index << endl;
  cout << "Owner: " << block->node_owner_number << endl;
  //cout << "Difficulty: " << block->difficulty << endl;
  //cout << "Created at: " << block->created_at << endl; 
  //cout << "Nonce: " << (string)block->nonce << endl;
  cout << "Previous block hash: " << (string)block->previous_block_hash << endl;
  cout << "Block hash: " << (string)block->block_hash << endl;
  cout << "--------------------" << endl;
}

void log_chain(string log_info){
  string filename = to_string((unsigned long) mpi_rank) + ".out";
  fstream outfile;
  outfile.open(filename, fstream::in | fstream::out | fstream::app);
  Block current = *last_block_in_chain;
  outfile << "Mi blockchain es la siguiente en " + log_info << endl;
  while(true){
    outfile << "--------------------" << endl;
    outfile << "Block number: " << current.index << endl;
    outfile << "Owner: " << current.node_owner_number << endl;
    outfile << "Previous block hash: " << (string)current.previous_block_hash << endl;
    outfile << "Block hash: " << (string)current.block_hash << endl;
    outfile << "--------------------" << endl;
    if(((string)current.previous_block_hash).empty()) break;
    current = node_blocks.at(((string)current.previous_block_hash));
  }

  outfile.close();
}

void log_msg(string msg){
  string filename = to_string((unsigned long) mpi_rank) + ".out";
  fstream outfile;
  outfile << "--------------------" << endl;
  outfile.open(filename, fstream::in | fstream::out | fstream::app);
  outfile << msg << endl;
  outfile << "--------------------" << endl;
  outfile.close();
}


void verify_chain_indexes(string label){
  bool good_indexes = true;
  unsigned int current_index = last_block_in_chain->index;
  Block* current_block_from_list = last_block_in_chain;

  while(true) {
    string prev_block_hash = current_block_from_list->previous_block_hash;

    if(current_index != current_block_from_list->index) good_indexes = false;
  
    if( prev_block_hash.empty() ) break;
  
    auto it = node_blocks.find(prev_block_hash);
    if (it != node_blocks.end()) {
      current_block_from_list = &(it->second);
    } else {
      cout << "[" + to_string(mpi_rank) + "]:" << label << "broken chain" << endl;
    }

    current_block_from_list = &node_blocks.at(prev_block_hash);
  }

  if(good_indexes) {
    cout << "[" + to_string(mpi_rank) + "]:" << label << "good indexes" << endl;
  } else {
    cout << "[" + to_string(mpi_rank) + "]:" << label << "bad indexes" << endl;
  }
}

bool sanity_test(){
  bool everything_ok = true;
  Block current = *last_block_in_chain;
  while(true){
    if(((string)current.previous_block_hash).empty()) break;
    everything_ok = ((node_blocks.find(((string)  current.previous_block_hash)) != node_blocks.end()) && (current.index - 1 == node_blocks.at(((string)current.previous_block_hash)).index)) && everything_ok;
    current = node_blocks.at(((string)current.previous_block_hash));
  }
  return everything_ok;
}

bool check_first(const Block *blockchain, const Block *rBlock){
  string hash_hex_str;
  block_to_hash(&blockchain[0],hash_hex_str);
  return ((blockchain[0].index == rBlock->index) && (string(blockchain[0].block_hash).compare(rBlock->block_hash) == 0) && (string(blockchain[0].block_hash).compare(hash_hex_str) == 0));
}

bool check_chain(const Block *blockchain){
  bool check = true;
  for (int i = 0; i < VALIDATION_BLOCKS; ++i){
    if(((string)blockchain[i].previous_block_hash).empty()) break;
    check = ((!((string)blockchain[i].previous_block_hash).compare( ((string)blockchain[i+1].block_hash))) 
      && (blockchain[i].index - 1 == blockchain[i+1].index) && check);
  }
  return check;
}

bool equal(Block block1, Block block2){
  return ((block1.index == block2.index) && (block1.node_owner_number == block2.node_owner_number) && (block1.difficulty == block2.difficulty) && (block1.created_at == block2.created_at) && !((string)block1.nonce).compare((string)block2.nonce) && !((string)block1.previous_block_hash).compare((string)block2.previous_block_hash) && !((string)block1.block_hash).compare((string)block2.block_hash));
}

bool look_for_block(Block block){
  Block current = *last_block_in_chain;
  while(true){
    if(equal(current, block)) return true;
    if(((string)current.previous_block_hash).empty()) break;
    current = node_blocks.at(((string)current.previous_block_hash));
  }
  return false;
}

// 1) Si devuelve -1 no encontró nada 
// 2) Si encontró un elemento en común entre la blockchain nueva y la que ya se tenía o llegó al bloque con index 1, devuelve la posición en blockchain
int find_block(const Block *blockchain, const map<string,Block> &node_blocks){
  for (int i = 0; i < VALIDATION_BLOCKS; ++i){
    if(look_for_block(blockchain[i]) || (blockchain[i].index == 1)) return i;
  }
  return -1;
}

//Cuando me llega una cadena adelantada, y tengo que pedir los nodos que me faltan
//Si nos separan más de VALIDATION_BLOCKS bloques de distancia entre las cadenas, se descarta por seguridad
bool verificar_y_migrar_cadena(const Block *rBlock, const MPI_Status *status){

  //Enviar mensaje TAG_CHAIN_HASH
  MPI_Send(rBlock, 1, *MPI_BLOCK, rBlock->node_owner_number, TAG_CHAIN_HASH, MPI_COMM_WORLD);

  Block *received_blockchain = new Block[VALIDATION_BLOCKS];

  //Recibir mensaje TAG_CHAIN_RESPONSE
  MPI_Status statusRes;
  MPI_Recv(received_blockchain, VALIDATION_BLOCKS, *MPI_BLOCK, rBlock->node_owner_number, TAG_CHAIN_RESPONSE, MPI_COMM_WORLD, &statusRes);

  bool received_blockchain_checks = check_first(received_blockchain, rBlock) && check_chain(received_blockchain);

  // 1) Si devuelve 0 no encontró nada 
  // 2) Si devuelve 1 entonces llegó al primero 
  int i = find_block(received_blockchain, node_blocks);
  
  cout << "[" + to_string(mpi_rank) + "]: find = " + to_string(i) + " | received_blockchain_checks = " + to_string(received_blockchain_checks) << endl;

  if(received_blockchain_checks && -1 < i) {
    
    // agrego las entradas de la nueva cadena
    for(int j = 0; j < i+1; j++){
      Block current_received_block = received_blockchain[j];

      node_blocks.insert({string(current_received_block.block_hash), current_received_block}); 

    }

    // seteo nuevo last elements    

    last_block_in_chain = &node_blocks.at(((string)received_blockchain[0].block_hash));

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
    node_blocks.insert({(string)rBlock->block_hash,*rBlock});
    
    //Si el índice del bloque recibido es 1
    //y mí último bloque actual tiene índice 0,
    //entonces lo agrego como nuevo último.
    if((rBlock->index==1) && (last_block_in_chain->index==0)){
      last_block_in_chain= (&node_blocks.at((string)rBlock->block_hash));
      printf("[%d] Agregado a la lista bloque con index %u enviado por %d \n", mpi_rank, rBlock->index, status->MPI_SOURCE);
      mined_blocks += 1;
      return true;
    }

    //Si el índice del bloque recibido es
    //el siguiente a mí último bloque actual,
    //y el bloque anterior apuntado por el recibido es mí último actual,
    //entonces lo agrego como nuevo último.
    if((rBlock->index==(last_block_in_chain->index)+1) && ((string)rBlock->previous_block_hash).compare((string)last_block_in_chain->block_hash) == 0){
      last_block_in_chain=  (&node_blocks.at((string)rBlock->block_hash));
      printf("[%d] Agregado a la lista bloque con index %u enviado por %d \n", mpi_rank, rBlock->index,status->MPI_SOURCE);
      mined_blocks += 1;
      return true;
    }

    //Si el índice del bloque recibido es
    //el siguiente a mí último bloque actual,
    //pero el bloque anterior apuntado por el recibido no es mí último actual,
    //entonces hay una blockchain más larga que la mía.
    if((rBlock->index==(last_block_in_chain->index)+1) && ((string)rBlock->previous_block_hash).compare(last_block_in_chain->block_hash) != 0){
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


void send_block_to_everyone(const Block block){
  int new_rank;
  cout << "[" + to_string(mpi_rank) + "]: total nodes " + to_string(total_nodes) + "." << endl;

  for(int i = 1; i < total_nodes; i++){
  
    new_rank = (mpi_rank + i) % total_nodes;

    cout << "[" + to_string(mpi_rank) + "]: sending to " + to_string(new_rank) + "." << endl;

    int send_return_status = MPI_Send(&block, 1, *MPI_BLOCK, new_rank, TAG_NEW_BLOCK, MPI_COMM_WORLD);

    cout << "[" + to_string(mpi_rank) + "]: sent to " + to_string(new_rank) + "." << endl;

    if(send_return_status != MPI_SUCCESS) {
      printf("[%d] send to node %d failed with error code %d \n",mpi_rank, new_rank, send_return_status);
    }
  }
}



//Proof of work 
void* proof_of_work(void *ptr){

    //cout << "Mi tid es " + to_string(pthread_self()) + " y el valor de tid es " + to_string(tid) << endl;


    string hash_hex_str;
    Block block;
    while(true){
      if(last_block_in_chain->index >= BLOCKS_TO_MINE){
        log_msg("Terminé con la siguiente cadena");
        log_chain("");
        break;
      }

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


          pthread_mutex_lock(&(_sendMutex));

          //Verifico que no haya cambiado mientras calculaba

          if(last_block_in_chain->index < block.index){

            mined_blocks += 1;
            strcpy(block.block_hash, hash_hex_str.c_str());
            node_blocks.insert({hash_hex_str, block}); //[hash_hex_str] = *last_block_in_chain;
            last_block_in_chain = &node_blocks.at(hash_hex_str);
            printf("[%d] Agregué un producido con index %u \n",mpi_rank,last_block_in_chain->index);

            // Mientras comunico, no respondo mensajes de nuevos nodos 
            send_block_to_everyone(*last_block_in_chain);

          }

      }
      pthread_mutex_unlock(&(_sendMutex));
    }
    MPI_Abort(MPI_COMM_WORLD,0);
    pthread_exit(0);
}

int send_blockchain(Block buffer, const MPI_Status *status){

  // asume VALIDATION_BLOCKS > 0
  // Defino la cadena a enviar => Voy a llenarla con:
  // min{blockchain.len, VALIDATION_BLOCKS} bloques hacia atrás desde el bloque recibido en buffer 

  unsigned int rank_of_asking_node = status->MPI_SOURCE; // 

  Block *blockchain = new Block[VALIDATION_BLOCKS];
  for (int i = 0; i < VALIDATION_BLOCKS; ++i){
    blockchain[i] = buffer;
    cout << "[" + to_string(mpi_rank) + "]: estoy agregando el bloque " + to_string(i) + " de " + to_string(mined_blocks) + " y el anterior tiene hash " + (string)buffer.previous_block_hash << endl;
    if(buffer.previous_block_hash == 0 ||(string(buffer.previous_block_hash).size()==0)){
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

void end_exc(int sig){
  cout << "Salgo" << endl;
  pthread_exit(0);
}

int node(){

  //Tomar valor de mpi_rank y de nodos totales
  MPI_Comm_size(MPI_COMM_WORLD, &total_nodes);
  MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);

  //La semilla de las funciones aleatorias depende del mpi_ranking
  srand(time(NULL) + mpi_rank);
  printf("[MPI] Lanzando proceso %u\n", mpi_rank);

  last_block_in_chain = initialize_first_block();
  Block *lastblock_reference = last_block_in_chain;

  pthread_t thread;
  tid = pthread_self();

  signal(3,end_exc);


  //Crear thread para minar
  pthread_create(&thread, NULL, proof_of_work, NULL);
  //cout << "Mi tid es " + to_string(pthread_self()) + " y el valor de tid es " + to_string(tid) << endl;
  while(true){

      //Recibir mensajes de otros nodos
      Block buffer;
      MPI_Status status;

      MPI_Recv(&buffer, 1, *MPI_BLOCK, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
      pthread_mutex_lock(&(_sendMutex));

      //Si es un mensaje de nuevo bloque, llamar a la función
      if(status.MPI_TAG==TAG_NEW_BLOCK){
        validate_block_for_chain(&buffer,&status);

      }else if(status.MPI_TAG==TAG_CHAIN_HASH){ //Si es un mensaje de pedido de cadena,
        printf("[%u] TAG_CHAIN_HASH \n", mpi_rank);

        send_blockchain(buffer, &status);

      }

      pthread_mutex_unlock(&(_sendMutex));

  }

  delete lastblock_reference; 
  return 0;
}
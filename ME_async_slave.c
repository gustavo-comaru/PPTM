#include <stdio.h>
#include <unistd.h>
#include "mpi.h"
#include <stdlib.h>
#include <time.h>

void bs(int n, int *vetor);
int cmpInt (const void * a, const void * b);

main(int argc, char ** argv) {

    #define TAREFAS 10 // Num de tarefas no saco de trabalho
    #define TAM 100

    //#define DEBUG 1
    #define BUBBLE 1

    int my_rank; /* Identificador do processo */
    int proc_n; /* Número de processos */
    int source; /* Identificador do proc.origem */
    int dest; /* Identificador do proc. destino */
    int tag; /* Tag para as mensagens */

    int i,j;            // Variaveis Iteradoras

    int atual=0;        // Indica linha a ser enviada p escravo
    int tagSTART=9900;   // Tag que indica inicio da execucao
    int tagKILL=9999;   // Tag que indica fim da execucao
    int linha;          // Indica a linha que deve se escrever
    int count=0;        // Contador de processos apos finalizar o trabalho

    static int saco[TAREFAS][TAM];  // saco de trabalho
    static int aux[TAM];            // buffer auxiliar

    MPI_Status status; /* Status de retorno */
    MPI_Request request; /* */
    MPI_Init( &argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &proc_n);

    double t1,t2;

    if ( my_rank == 0 ){    // papel do mestre
        t1 = MPI_Wtime();  // inicia a contagem do tempo
        srand(time(NULL));  // Inicializa a semente do rand()
        for ( j=0 ; j < TAREFAS ; j++) // Percorre a matriz
            for (i = 0; i < TAM; i++){ // Inserindo valores
                saco[j][i] = rand()%TAM; // Aleatorios
                #ifdef DEBUG
                printf("[%d][%d] = %d\n",j,i,saco[j][i]);
                #endif
            }
        #ifdef DEBUG
        printf("\n------------------------------------------\n");
        #endif
        
        while(1){                               // inicio o laço recebendo por ordem de chegada com any_source
            MPI_Irecv(aux, TAM, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &request);
            MPI_Wait(&request, &status);

            linha = status.MPI_TAG;    // A tag é a linha que o escravo pegou para ordenar

            if (linha !=tagSTART) {
                for ( j=0; j < TAM; j++){
                    saco[linha][j] = aux[j];   // coloco mensagem no saco na posição original
                    #ifdef DEBUG 
                    printf("[%d][%d] = %d\n", status.MPI_TAG,j,saco[status.MPI_TAG][j]);
                    #endif
                }
            }

            if(atual<TAREFAS)   // Se nao ordenei todas ainda entao envia para o escravo que finalizou
                MPI_Isend( &saco[atual], TAM, MPI_INT, status.MPI_SOURCE, atual, MPI_COMM_WORLD, &request);
                atual++;            // Incremento o contador de linhas ordenadas
            else{               // Caso tenha finalizado o saco
                count++;        // Conta o numero de escravos que finalizaram
                if(count==(proc_n-1))
                    break;      // Quando todos os escravos finalizarem então finaliza o laço
            }

        }

        for ( i=0 ; i < proc_n-1; i++) // Envia Kill para todos os escravos
            MPI_Isend( &saco[0], TAM, MPI_INT, i+1, 99999, MPI_COMM_WORLD, &request); // envio tag kill para os escravos

        t2 = MPI_Wtime(); // termina a contagem do tempo
        printf("\nTempo de execucao: %f\n\n", t2-t1); 

    }              

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    else{           // papel do escravo
        
        MPI_Isend(aux, TAM, MPI_INT, 0, tagSTART, MPI_COMM_WORLD, &request);
        
        while(1){

            MPI_Irecv(aux, TAM, MPI_INT, 0, MPI_ANY_TAG, MPI_COMM_WORLD, &request);    // recebo do mestre
            MPI_Wait(&request, &status);

            tag = status.MPI_TAG; // Testo a tag, se nao for de kill ordeno o vetor
            if(tag==99999)
                break;

            #ifdef BUBBLE       // Escolho o algoritmo baseado em flag global
            bs(TAM, aux);       // Bubble sort
            #endif
            
            #ifndef BUBBLE      // Escolho o algoritmo baseado em flag global
            qsort(aux, TAM, sizeof(int), cmpInt); // Quick Sort
            #endif

            // Retorno o resultado ao mestre com a tag sendo a mesma que recebi e que eh a linha da matriz
            MPI_Isend(aux, TAM, MPI_INT, 0, status.MPI_TAG, MPI_COMM_WORLD, &request);
        }
    }

    MPI_Finalize();  
}

void bs(int n, int * vetor){
    int c=0, d, troca, trocou =1;
    while (c < (n-1) & trocou){
        trocou = 0;
        for (d = 0 ; d < n - c - 1; d++)
            if (vetor[d] > vetor[d+1]){
                troca      = vetor[d];
                vetor[d]   = vetor[d+1];
                vetor[d+1] = troca;
                trocou = 1;
            }
        c++;
        }
    }

int cmpInt (const void * a, const void * b){
   return ( *(int*)a - *(int*)b );
}

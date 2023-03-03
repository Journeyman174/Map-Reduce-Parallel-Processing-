#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <math.h>

typedef struct celula { 
	int info;
  	struct celula * urm;
} TCelula, *TLista;      //tipurile Celula, Lista, utilizata pt memorarea listelor cu puteri perfecte

typedef struct {
	char **numef; 	  //nume doc text de testat
	int procesat; 	 //stare doc
	TLista *map;	//pointer catre listele partiale
	int nr_liste;  //numar liste partiale = nr thread-uri reduce
	int th_map;
} docMAP;		 //structura utilizata de thread MAP


typedef struct {
	docMAP *arrMAP;  //vector doc map
	TLista r;		//listele agregate
	int id_RED;	   //pointer numar thread reduce
	int nr_MAP;	  //NUMAR THREADS MAP
	char numef[250];
} docRED;		//structura utilizata de thread REDUCE

pthread_barrier_t barrier;

//functii liste simplu inlantuite

TLista AlocCelula(int e) {          				/* adresa celulei create sau NULL */
	TLista aux = (TLista)malloc(sizeof(TCelula));  /* (1) incearca alocare */
	if (aux) {                            		  /* aux contine adresa noii celule */
		aux->info = e;                   		 /* (2,3) completeaza noua celula */
		aux->urm = NULL;
  	}
  return aux;             					/* rezultatul este adresa noii celule sau NULL */
}

void AfisareL(TLista L) {
	/* afiseaza valorile elementelor din lista */
	
	printf("Lista: [");                      /* marcheaza inceput lista */

	for (; L != NULL; L = L->urm)       	/* pentru fiecare celula */
		printf("%d ", L->info);            /* afiseaza informatie */

	printf("]");                          /* marcheaza sfarsit lista */
}


void DistrugeL(TLista* aL) {
	TLista aux;
	while(*aL) {
		aux = *aL;
		*aL = aux->urm;
		free(aux);
	}
}

int putereP2(int x, int n) {
	float r, r1;

	if(x == 1) return 1;
	if(x == 0) return 0;

	r = exp(log(x) / n);
	r1 = trunc(exp(log(x) / n));

	if (((r - r1) == 0) && (x == pow(r, n))) 
		return 1;
	return 0;
}

int putereP3(int x, int n) {
//verifica daca nr. e putere perfecta
//intoarce 1 daca e putere perfecta

	float r1;
	int p;
	if(x == 1) return 1;
	if(x == 0) return 0;
	r1 = trunc(exp(log(x) / n));

	for (int i = r1 - 10; i < r1 + 10; i++) {
		p = i;
		for (int j = 1; j < n; j++) {
			p *= i;
		}
		if (p == x) {
			return 1;
		}
	}
	return 0;
}

int putereP(int x, int exp) {
//verifica daca nr. e putere perfecta
//intoarce 1 daca e putere perfecta
	int p;
	if(x == 1) return 1;
	if(x == 0) return 0;
	for (int i = 2; i < x; i++) {
		p = i;
		for (int j = 1; j < exp; j++){
			p *= i;
		}
		if (p == x) {
			return 1;
		}
	}
	return 0;
}

/****************************/

//functia apelata de thread MAP

void *fm(void *arg) {
	docMAP *doc = (docMAP *)arg;
	TLista u, aux;
	FILE * f_in;
 
	int n, x, k = 0;

	while (doc->numef[k] != NULL) {
		f_in = fopen(doc->numef[k], "rt");
		if(f_in == NULL) return NULL;

		doc->procesat = 1;	//marchez doc ca prelucrat
		fscanf(f_in, "%d", &n);
		//citesc doc text in
		for (int i = 0; i < n; ++i) {
			fscanf(f_in, "%d", &x);
			for (int j = 0; j < doc->nr_liste; j++) {
				if (putereP3(x, j + 2) == 1) {
					aux = AlocCelula(x);           			   //incearca inserarea valorii citite 
					if(!aux) return doc->map[j];              //alocare esuata => sfarsit citire 
					if(doc->map[j] == NULL) {
						doc->map[j] = aux;
					}
  			  		else {
  			  			for (u = doc->map[j];  u->urm != NULL; u = u->urm); 
  		  				u->urm = aux;
					}
        		}
  			}
   		} 
		fclose(f_in);
		k++;
	}
	pthread_barrier_wait(&barrier);
	pthread_exit(NULL);
}

/**********************************/

//functie apelata de thread REDUCE
void *fr(void *arg) {
	TLista u;
	FILE * f_out ;
	docRED *red = (docRED *)arg;

	pthread_barrier_wait(&barrier);
	
	red->r = (red->arrMAP[0]).map[red->id_RED];
	for(int i = 1; i < red->nr_MAP; i++) {
		if(red->r == NULL) {
			red->r = (red->arrMAP[i]).map[red->id_RED];
		}
		else {
			for(u = red->r; u->urm != NULL; u = u->urm);
			u->urm = red->arrMAP[i].map[red->id_RED];
		}
	}

	TLista ant, v;
	int x, rez = 0;

	for(u = red->r; u != NULL; u = u->urm) {
		x = u->info;
		ant = u;
		for(v = u->urm; v != NULL; v = v->urm) {
			if (x == v->info) {
				ant->urm = v->urm;
			}
		else 
			ant = v;
		}
	}

	for(u = red->r; u != NULL; u = u->urm)
		rez++;	//nr. elem unice

	f_out = fopen(red->numef, "wt");
	fprintf(f_out,"%d", rez);
	fclose(f_out);

	pthread_exit(NULL);
}

/************************************/

int main(int argc, char *argv[]) {
	char *numefis_test, numefis[250];
	FILE * f_in;
	int nr_threads_m = atoi(argv[1]); 	 //argument 1 - nr. thread-uri Mapper
	int nr_threads_r = atoi(argv[2]); 	//argument 2 - nr. threds-uri Reducer
    numefis_test = argv[3];            //argument 3 - nume fisier test
	
	if(argc < 2) {
    	perror("Sintaxa eronata : ./tema1 3 5 ./test0/test.txt\n");
    	exit(-1);
  	}
  
	 //citire fis test si creare array doc.
	//deschid fis de intrare si fis de iesire
    
	f_in = fopen(numefis_test, "rt");
    if(f_in == NULL) 	
		return 1;

    int n, r, i, j, k; //numarul de elemente && index

	//citesc n din fisier
    fscanf(f_in, "%d", &n);
    docMAP arr_f_test[nr_threads_m]; //array documente text e procesat map
    docRED arr_red[nr_threads_r];	//array  liste agregate    

    //citesc doc de test
	for (i = 0; i < nr_threads_m; i++) {
		arr_f_test[i].numef = malloc(5000 * sizeof(char*));	          //vector nume fis alocate thread
		arr_f_test[i].map = malloc(sizeof(TLista *) * nr_threads_r); //vector de pointer la listele partiale
    }

    for (i = 0, j = 0; i < n; ++i, j++) {
		if (j < nr_threads_m) {
			fscanf(f_in, "%s", numefis);	//citesc numele doc text
			k = 0;
			while(arr_f_test[j].numef[k] != NULL) {
				k++;
			} //ma duc la sfarsit vector fisiere

			arr_f_test[j].numef[k] = malloc(5000 * sizeof(char*)); //vector nume fis alocate thread
			strcpy(arr_f_test[j].numef[k], numefis);
		
			arr_f_test[j].procesat = 0;	//marchez ca neprocesat
       		arr_f_test[j].nr_liste = nr_threads_r; //nr de liste partiale = nr. exponenti = nr. threads reduce
  			arr_f_test[j].th_map = j;
    	}
    	else {
			j = 0;
			fscanf(f_in, "%s", numefis);	//citesc numele doc text
			k = 0;
			while(arr_f_test[j].numef[k] != NULL) {
				k++;
			} //ma duc la sfirsit vector fisiere

			arr_f_test[j].numef[k] = malloc(5000 * sizeof(char*)); //vector nume fis alocate thread
			strcpy(arr_f_test[j].numef[k], numefis);
    	}
    }

    fclose(f_in);
	char cale[250];
	char num[10];

	for (i = 0; i < nr_threads_r; i++) {
		arr_red[i].arrMAP = arr_f_test;	//pointer liste partiale
		arr_red[i].nr_MAP = nr_threads_m;
		arr_red[i].r = arr_f_test[0].map[i];
		arr_red[i].id_RED = i;
		strcpy(arr_red[i].numef, cale);
	
		strcat(arr_red[i].numef,"./out");
		sprintf(num, "%d", i + 2);
		strcat(arr_red[i].numef, num);
		strcat(arr_red[i].numef, ".txt");
	}

	pthread_t threads[nr_threads_m + nr_threads_r];
	long id;

	pthread_barrier_init(&barrier, NULL, nr_threads_m + nr_threads_r);

	for (id = 0; id <= nr_threads_m + nr_threads_r - 1; id++) {
		if(id < nr_threads_m) {
			r = pthread_create(&threads[id], NULL, fm, (void *)&arr_f_test[id]);
    		if (r) {
      			printf("Eroare la crearea thread-ului map %ld\n", id);
      			exit(-1);
    		}
		}
		else {
    		r = pthread_create(&threads[id], NULL, fr, (void *)&arr_red[id - nr_threads_m]);
			if (r) {
      			printf("Eroare la crearea thread-ului map %ld\n", id);
      			exit(-1);
    		}
		}
  	}

	//se asteapta terminarea thread-urilor
	for (id = 0; id <= nr_threads_m + nr_threads_r - 1; id++) { 
		r = pthread_join(threads[id], NULL);
		if (r) {
			printf("Eroare la asteptarea thread-ului %ld\n", id);
			exit(-1);
		}
	}

	pthread_barrier_destroy(&barrier);

	for (i = 0; i < nr_threads_r; i++) {
		DistrugeL(&arr_red[i].r);	//eliberare memorie
	}

	return 0;
}
#include <bits/stdc++.h>
#include <mpi.h>
using namespace std;

typedef struct node{
  char *val;
  float key;
}node;

void get_emp(int n, char *a){
  for(int i=0;i<n;i++){
    a[i] = '\0';
  }
}

bool compareNode(node i1, node i2)
{
    return (i1.key < i2.key);
}


stringstream filewrite;

int main(int argc, char **argv)
{
  MPI_Init(&argc, &argv);
  
  int rank, size;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &size);

  int num_files = atoi(argv[1]);
  char *baseline = argv[2];

  if(num_files%size!=0){
    num_files = size*(num_files/size + 1);
  }

  int ite_str = rank*(num_files/size);
  int ite_end = ite_str + num_files/size;

  int ite_len = ite_end - ite_str;

  long *arr_file_size = (long *)malloc(ite_len*sizeof(long));
  size_t result;

  int *my_max = (int *)malloc(2*sizeof(int));
  my_max[0] = 0;
  my_max[1] = 0;
  

  for(int ite=ite_str;ite<ite_end;ite++){
    int curr_ite = ite - ite_str;

    int *n_buffer;

    stringstream ss;
    ss << baseline;
    ss << ite+1;
    string filename = ss.str();

    FILE *pFile = fopen (filename.c_str(), "rb" );
    if (pFile==NULL){
      continue;
    }

    // obtain file size:
    fseek (pFile , 0 , SEEK_END);
    arr_file_size[curr_ite] = ftell (pFile);
    rewind (pFile);

    n_buffer = (int*) malloc (4);
    result = fread (n_buffer,4,1,pFile);
    rewind (pFile);
    int n = n_buffer[0];

    int num_keys = (arr_file_size[curr_ite]-4)/(4+n);
    if(num_keys>my_max[0]){
      my_max[0] = num_keys;
    }
    if(n>my_max[1]){
      my_max[1] = n;
    }
    fclose(pFile);
  }

  int *all_keys;
  if (rank == 0) {
    all_keys = (int *)malloc(sizeof(int) * size * 2);
  }

  MPI_Gather(my_max, 2, MPI_INT, all_keys, 2, MPI_INT, 0, MPI_COMM_WORLD);

  int max_keys;
  int max_n;
  if(rank==0){
    max_keys = all_keys[0];
    for(int i=0;i<2*size;i=i+2){
      if(all_keys[i]>max_keys){
        max_keys = all_keys[i];
      }
    }
    if(max_keys%size!=0){
      max_keys = size*(max_keys/size + 1);
    }
    max_n = all_keys[1];
    for(int i=1;i<2*size;i=i+2){
      if(all_keys[i]>max_n){
        max_n = all_keys[i];
      } 
    }
  }

  MPI_Bcast(&max_keys, 1, MPI_INT, 0, MPI_COMM_WORLD);
  MPI_Bcast(&max_n, 1, MPI_INT, 0, MPI_COMM_WORLD);

  float *my_col_keys = (float*) malloc (4*max_keys*ite_len);
  char *my_col_vals = (char*) malloc (max_n*max_keys*ite_len);

  for(int ite=ite_str;ite<ite_end;ite++){
    int curr_ite = ite - ite_str;

    int num_keys;
    int n;
    int some_flag = 1;

    stringstream ss;
    ss << baseline;
    ss << ite+1;
    string filename = ss.str();

    FILE *pFile = fopen (filename.c_str(), "rb" );
    
    if(pFile==NULL){
      num_keys = 0;
      n = 0;
      some_flag = 0;
    }
    else{
      int *n_buffer;

      n_buffer = (int*) malloc (4);
      result = fread(n_buffer,4,1,pFile);
      n = n_buffer[0];

      num_keys = (arr_file_size[curr_ite]-4)/(4+n);
    }

    my_col_keys += curr_ite;
    my_col_vals += curr_ite*max_n;

    for(int i=0;i<num_keys;i++)
    {
      result = fread (my_col_keys,4,1,pFile);
      my_col_keys += ite_len;
      result = fread (my_col_vals,1,n,pFile);
      get_emp(max_n-n, my_col_vals+n);
      my_col_vals += ite_len*max_n;
    }

    for(int i=num_keys;i<max_keys;i++)
    {
      *my_col_keys = FLT_MAX;
      my_col_keys += ite_len;
      get_emp(max_n, my_col_vals);
      my_col_vals += ite_len*max_n;
    }

    my_col_keys -= curr_ite;
    my_col_vals -= curr_ite*max_n;

    my_col_keys -= ite_len*max_keys;
    my_col_vals -= ite_len*max_keys*max_n;

    if(some_flag==1){
      fclose (pFile);
    }
  }

  int all_done = 0;

  float *my_row_keys = (float*) malloc (4*(max_keys/size)*num_files);
  char *my_row_vals = (char*) malloc(max_n*(max_keys/size)*num_files);

  while(all_done==0){
    for(int i=0;i<size;i++){
      MPI_Scatter(my_col_keys, ite_len*(max_keys/size), MPI_FLOAT, my_row_keys, ite_len*(max_keys/size), MPI_FLOAT, i, MPI_COMM_WORLD);
      my_row_keys += ite_len*(max_keys/size);
      MPI_Scatter(my_col_vals, ite_len*max_n*(max_keys/size), MPI_CHAR, my_row_vals, ite_len*max_n*(max_keys/size), MPI_CHAR, i, MPI_COMM_WORLD);
      my_row_vals += ite_len*max_n*(max_keys/size);
    }

    my_row_keys -= (max_keys/size)*num_files;
    my_row_vals -= (max_keys/size)*num_files*max_n;

    int row_done = 1;

    for(int i=0;i<max_keys/size;i++){

      node *the_arr = (node *)malloc(num_files*sizeof(node));
      my_row_keys += i*ite_len;
      my_row_vals += i*ite_len*max_n;
      for(int j=0;j<size;j++){
        for(int l=0;l<ite_len;l++){
          int ind = j*ite_len + l;
          the_arr[ind].val = (char *)malloc(max_n);
          for(int p=0;p<max_n;p++){
            the_arr[ind].val[p] = my_row_vals[p];
          }
          the_arr[ind].key = *my_row_keys;
          my_row_keys += 1;
          my_row_vals += max_n;
        }
        my_row_keys += ite_len*(max_keys/size) - ite_len;
        my_row_vals += (ite_len*(max_keys/size) - ite_len)*max_n;
      }

      int flag = 0;
      for(int j=1;j<num_files;j++){
        if(compareNode(the_arr[j], the_arr[j-1])){
          flag = 1;
          break;
        }
      }

      if(flag==1){
        sort(the_arr, the_arr + num_files, compareNode);
        row_done = 0;
      }


      my_row_keys -= ite_len*(max_keys/size)*size;
      my_row_vals -= ite_len*(max_keys/size)*size*max_n;
      for(int j=0;j<size;j++){
        for(int l=0;l<ite_len;l++){
          int ind = j*ite_len + l;
          for(int p=0;p<max_n;p++){
            my_row_vals[p] = the_arr[ind].val[p];
          }
          *my_row_keys = the_arr[ind].key;
          my_row_keys += 1;
          my_row_vals += max_n;
        }
        my_row_keys += ite_len*(max_keys/size) - ite_len;
        my_row_vals += (ite_len*(max_keys/size) - ite_len)*max_n;
      }

      my_row_keys -= ite_len*(max_keys/size)*size + i*ite_len;
      my_row_vals -= (ite_len*(max_keys/size)*size + i*ite_len)*max_n;

    }

    for(int i=0;i<size;i++){
      MPI_Scatter(my_row_keys, ite_len*(max_keys/size), MPI_FLOAT, my_col_keys, ite_len*(max_keys/size), MPI_FLOAT, i, MPI_COMM_WORLD);
      my_col_keys += ite_len*(max_keys/size);
      MPI_Scatter(my_row_vals, ite_len*max_n*(max_keys/size), MPI_CHAR, my_col_vals, ite_len*max_n*(max_keys/size), MPI_CHAR, i, MPI_COMM_WORLD);
      my_col_vals += ite_len*max_n*(max_keys/size);
    }

    my_col_keys -= max_keys*ite_len;
    my_col_vals -= max_keys*ite_len*max_n;

    int col_done = 1;

    for(int i=0;i<ite_len;i++){

      node *the_arr = (node *)malloc(max_keys*sizeof(node));
      my_col_keys += i;
      my_col_vals += i*max_n;
      for(int j=0;j<max_keys;j++){
        int ind = j;
        the_arr[ind].val = (char *)malloc(max_n);
        for(int p=0;p<max_n;p++){
          the_arr[ind].val[p] = my_col_vals[p];
        }
        the_arr[ind].key = *my_col_keys;
        my_col_keys += ite_len;
        my_col_vals += ite_len*max_n;
      }

      int flag = 0;
      for(int j=1;j<max_keys;j++){
        if(compareNode(the_arr[j], the_arr[j-1])){
          flag = 1;
          break;
        }
      }

      if(flag==1){
        sort(the_arr, the_arr + max_keys, compareNode);
        col_done = 0;
      }

      my_col_keys -= ite_len*max_keys;
      my_col_vals -= ite_len*max_keys*max_n;
      for(int j=0;j<max_keys;j++){
        int ind = j;
        for(int p=0;p<max_n;p++){
          my_col_vals[p] = the_arr[ind].val[p];
        }
        *my_col_keys = the_arr[ind].key;
        my_col_keys += ite_len;
        my_col_vals += ite_len*max_n;
      }

      my_col_keys -= (ite_len*max_keys + i);
      my_col_vals -= (ite_len*max_keys + i)*max_n;

    }

    int *my_dones = (int *)malloc(sizeof(int));
    my_dones[0] = row_done;
    my_dones[1] = col_done;
    int *all_dones;
    if (rank == 0) {
      all_dones = (int *)malloc(sizeof(int) * size * 2);
    }

    MPI_Gather(my_dones, 2, MPI_INT, all_dones, 2, MPI_INT, 0, MPI_COMM_WORLD);

    if(rank==0){
      all_done = 1;
      for(int i=0;i<2*size;i++){
        if(all_dones[i]==0){
          all_done = 0;
          break;
        }
      }
    }

    MPI_Bcast(&all_done, 1, MPI_INT, 0, MPI_COMM_WORLD);
  }

  for(int i=0;i<size;i++){
    MPI_Scatter(my_col_keys, ite_len*(max_keys/size), MPI_FLOAT, my_row_keys, ite_len*(max_keys/size), MPI_FLOAT, i, MPI_COMM_WORLD);
    my_row_keys += ite_len*(max_keys/size);
    MPI_Scatter(my_col_vals, ite_len*max_n*(max_keys/size), MPI_CHAR, my_row_vals, ite_len*max_n*(max_keys/size), MPI_CHAR, i, MPI_COMM_WORLD);
    my_row_vals += ite_len*max_n*(max_keys/size);
  }

  my_row_keys -= (max_keys/size)*num_files;
  my_row_vals -= (max_keys/size)*num_files*max_n;

  if(rank==0){
    stringstream ss;
    ss << baseline;
    ss << '0';
    string filename = ss.str();

    FILE *pFile = fopen (filename.c_str(), "wb");
    for(int i=0;i<max_keys/size;i++){
      my_row_keys += i*ite_len;
      my_row_vals += i*ite_len*max_n;
      for(int j=0;j<size;j++){
        for(int l=0;l<ite_len;l++){
          int ind = j*ite_len + l;

          if(my_row_keys[0]==FLT_MAX){
            my_row_keys += 1;
            my_row_vals += max_n;
            continue;
          }
          fwrite(&my_row_keys[0], 4, 1, pFile);
          
          for(int p=0;p<max_n;p++){
            char temp =my_row_vals[p];
            if(temp=='\0'){
              break;
            }
            fwrite(&temp, 1, 1, pFile);
          }

          my_row_keys += 1;
          my_row_vals += max_n;
          
        }
        my_row_keys += ite_len*(max_keys/size) - ite_len;
        my_row_vals += (ite_len*(max_keys/size) - ite_len)*max_n;
      }
      my_row_keys -= ite_len*(max_keys/size)*size + i*ite_len;
      my_row_vals -= (ite_len*(max_keys/size)*size + i*ite_len)*max_n;

    }

    for(int i=1;i<size;i++){
      MPI_Recv(my_row_keys, (max_keys/size)*num_files, MPI_FLOAT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
      MPI_Recv(my_row_vals, (max_keys/size)*num_files*max_n, MPI_CHAR, i, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
      for(int i=0;i<max_keys/size;i++){
        my_row_keys += i*ite_len;
        my_row_vals += i*ite_len*max_n;
        for(int j=0;j<size;j++){
          for(int l=0;l<ite_len;l++){
            int ind = j*ite_len + l;

            if(my_row_keys[0]==FLT_MAX){
              my_row_keys += 1;
              my_row_vals += max_n;
              continue;
            }
            fwrite(&my_row_keys[0], 4, 1, pFile);
            
            for(int p=0;p<max_n;p++){
              char temp =my_row_vals[p];
              if(temp=='\0'){
                break;
              }
              fwrite(&temp, 1, 1, pFile);
            }

            my_row_keys += 1;
            my_row_vals += max_n;
            
          }
          my_row_keys += ite_len*(max_keys/size) - ite_len;
          my_row_vals += (ite_len*(max_keys/size) - ite_len)*max_n;
        }
        my_row_keys -= ite_len*(max_keys/size)*size + i*ite_len;
        my_row_vals -= (ite_len*(max_keys/size)*size + i*ite_len)*max_n;

      }
    }
  }

  else{
    MPI_Send(my_row_keys, (max_keys/size)*num_files, MPI_FLOAT, 0, 0, MPI_COMM_WORLD);
    MPI_Send(my_row_vals, (max_keys/size)*num_files*max_n, MPI_CHAR, 0, 1, MPI_COMM_WORLD);
  }

  MPI_Finalize();
  return 0;
}

#include <bits/stdc++.h>
#include <mpi.h>
using namespace std;

typedef struct node{
    int r;
    int c;
    int cval;
    float rval;
}node;

bool dataSort(node i1, node i2)
{
    if(i1.c==i2.c){
        return (i1.r < i2.r);
    }
    else{
        return (i1.c < i2.c);
    }
}

bool positionCompare(pair<int, int> i1, pair<int, int> i2)
{
    return (i1.second < i2.second);
}

bool compareNodeRow(node i1, node i2)
{
    if(i1.rval == i2.rval){
        return (i1.c < i2.c);
    }
    else{
        return (i1.rval < i2.rval);
    }
}

bool compareNodeCol(node i1, node i2)
{
    if(i1.cval == i2.cval){
        return (i1.r < i2.r);
    }
    else{
        return (i1.cval < i2.cval);
    }
}

int main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);
  
    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    char *input_file = argv[1];
    char *output_file = argv[2];
    int num_rows = atoi(argv[1]);
    int num_cols = atoi(argv[1]);

    if(num_rows%size!=0){
        num_rows = size*(num_rows/size + 1);
    }

    if(num_cols%size!=0){
        num_cols = size*(num_cols/size + 1);
    }

    int ite_str = rank*(num_cols/size);
    int ite_end = ite_str + num_cols/size;

    int ite_len = ite_end - ite_str;

    FILE *pFile = fopen(input_file, "rb" );
    if (pFile==NULL){
        cout<<"No File Found named : "<<input_file<<endl;
        return;
    }

    long file_size;

    fseek (pFile , 0 , SEEK_END);
    file_size = ftell (pFile);
    rewind (pFile);

    int num_ele = file_size/(3*sizeof(int) + sizeof(float));

    int curr_size = num_ele/size;

    node *data = (node*) malloc(curr_size*sizeof(node));

    size_t result;

    int filled_size = 0;

    int *ele_in_bloc = (int *)malloc(size*sizeof(int));

    for(int i=0;i<num_ele;i++)
    {
        int temp;
        result = fread (&temp, sizeof(int), 1, pFile);
        if(temp<ite_end && temp>=ite_str)
        {

            if(filled_size==curr_size){
                curr_size = 2*curr_size;
                data = (node*) realloc (data, curr_size*sizeof(node));
            }

            data[filled_size].r = temp;
            result = fread (&temp, sizeof(int), 1, pFile);
            data[filled_size].c = temp;
            ele_in_bloc[c/ite_len] += 1;
            result = fread(&temp, sizeof(int), 1, pFile);
            data[filled_size].cval = temp;
            float tpflt;
            result = fread(&tpflt, sizeof(float), 1, pFile);
            data[filled_size].rval = tpflt;


            filled_size += 1;
        }
        else
        {
            pFile += 2*sizeof(int) + sizeof(float);
        }
    }

    data = (node*) realloc (data, filled_size*sizeof(node));

    fclose (pFile);

    int all_done = 0;

    int ite = 0;

    sort(data, dataSort);

    while(all_done==0 && ite<4)
    {
        omp pragma parallel for
        for(int i=ite_str;i<ite_end;i++){
            vector <pair <int, int> > glb_pos;
            vector <node> sort_val;
            glb_len = 0;
            for(int j=0;j<filled_size;j++){
                if(data[j].r==i){
                    glb_len += 1;
                    glb_pos.push_back(make_pair(j, data[j].c));
                    sort_val.push_back(data[j]);
                }
            }
            sort(glb_pos, positionCompare);
            sort(sort_val, compareNodeRow);
            for(int j=0;j<glb_len;j++){
                data[glb_pos[j][0]].rval = sort_val[j].rval;
                data[glb_pos[j][0]].cval = sort_val[j].cval;
            }
        }

        

        ite += 1;
    }
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

#include <bits/stdc++.h>
#include <mpi.h>
using namespace std;

typedef struct node{
    int r;
    int c;
    int cval;
    float rval;
}node;

typedef struct pos{
    int r;
    int c;
}pos;

bool dataRowSort(node i1, node i2)
{
    if(i1.r==i2.r){
        return (i1.c < i2.c);
    }
    else{
        return (i1.r < i2.r);
    }
}

bool posRowSort(pos i1, pos i2)
{
    if(i1.r==i2.r){
        return (i1.c < i2.c);
    }
    else{
        return (i1.r < i2.r);
    }
}

bool dataColSort(node i1, node i2)
{
    if(i1.r==i2.r){
        return (i1.c < i2.c);
    }
    else{
        return (i1.r < i2.r);
    }
}

bool posColSort(pos i1, pos i2)
{
    if(i1.r==i2.r){
        return (i1.c < i2.c);
    }
    else{
        return (i1.r < i2.r);
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
    int num_rows = atoi(argv[3]);
    int num_cols = atoi(argv[4]);

    if(num_rows%size!=0){
        num_rows = size*(num_rows/size + 1);
    }

    if(num_cols%size!=0){
        num_cols = size*(num_cols/size + 1);
    }

    int ite_rstr = rank*(num_rows/size);
    int ite_rend = ite_rstr + num_rows/size;

    int ite_rlen = ite_rend - ite_rstr;

    int ite_cstr = rank*(num_cols/size);
    int ite_cend = ite_cstr + num_cols/size;

    int ite_clen = ite_cend - ite_cstr;

    FILE *pFile = fopen(input_file, "rb" );
    if (pFile==NULL){
        cout<<"No File Found named : "<<input_file<<endl;
        return;
    }

    long file_size;

    fseek (pFile , 0 , SEEK_END);
    file_size = ftell (pFile);
    rewind (pFile);

    int num_ele = file_size/sizeof(node);

    int curr_size = num_ele/size;

    node *data = (node*) malloc(curr_size*sizeof(node));
    pos *all_pos = (pos*) malloc(curr_size*sizeof(pos));

    size_t result;

    int filled_size = 0;

    int *ele_in_col_bloc = (int *)malloc(size*sizeof(int));
    int *ele_in_row_bloc = (int *)malloc(size*sizeof(int));

    for(int i=0;i<num_ele;i++)
    {
        int temp;
        result = fread (&temp, sizeof(int), 1, pFile);
        if(temp<ite_end && temp>=ite_str)
        {
			if(filled_size==curr_size){
				curr_size = 2*curr_size;
				data = (node*) realloc (data, curr_size*sizeof(node));
				all_pos = (pos*) realloc (all_pos, curr_size*sizeof(pos));
			}

			data[filled_size].r = temp;
			all_pos[filled_size].r = temp;
			result = fread (&temp, sizeof(int), 1, pFile);
			data[filled_size].c = temp;
			all_pos[filled_size].c = temp;
			ele_in_col_bloc[temp/ite_clen] += 1;
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
    all_pos = (pos*) realloc (all_pos, filled_size*sizeof(pos));

    fclose (pFile);

    int all_done = 0;

    int ite = 0;

    for(int i=0;i<size;i++){
		MPI_Scatter(ele_in_col_bloc, 1, MPI_INT, &ele_in_row_bloc[i], 1, MPI_INT, i, MPI_COMM_WORLD);
	}

	col_filled_size = 0;
	for(int i=0;i<size;i++){
		col_filled_size += ele_in_col_bloc[i];
	}


    col_data = (node *)malloc(col_filled_size*sizeof(node));
    col_all_pos = (pos *)malloc(col_filled_size*sizeof(pos));

    sort(data, data + filled_size, dataRowSort);
    sort(all_pos, all_pos + filled_size, posRowSort);

    int *ele_in_row = (int *)malloc(ite_rlen*sizeof(int));
    for(int i=0;i<filled_size;i++){
    	int temp = all_pos[i].r;
		ele_in_row[temp - ite_rstr] += 1;
	}

    int pre_sum = 0;
    for(int i=0;i<ite_rlen;i++){
    	pre_sum += ele_in_row[i];
    	ele_in_row[i] = pre_sum;
    }

    while(all_done==0 && ite<4)
    {
        omp pragma parallel for
        for(int i=ite_rstr;i<ite_rend;i++){
            int start;
        	if(i==ite_rstr)
        		start = 0;
        	else
        		start = ele_in_row[i - ite_rstr - 1];
        	int end = ele_in_row[i - ite_rstr];

            sort(data + start, data + end, compareNodeRow);

            for(int j=start;j<end;j++){
                data[j].r = all_pos[j].r;
                data[j].c = all_pos[j].c;
            }
        }

        sort(data, data + filled_size, dataColSort);
        sort(all_pos, all_pos + filled_size, posColSort);
    	
    	int *col_bloc_pref = (int *)malloc(size*sizeof(int));
    	pref_sum = 0;
    	for(int i=0;i<size;i++){
    		int temp = ele_in_col_bloc[i];
    		col_bloc_pref[i] = pref_sum;
    		pref_sum += temp;
    	}

        for(int i=0;i<size;i++){
        	MPI_Scatterv(data, ele_in_col_bloc, col_bloc_pref, MPI_data, col_data, ele_in_row_bloc[i], MPI_data, i, MPI_COMM_WORLD);
        	col_data += ele_in_row_bloc[i];
        	MPI_Scatterv(all_pos, ele_in_col_bloc, col_bloc_pref, MPI_pos, col_all_pos, ele_in_row_bloc[i], MPI_pos, i, MPI_COMM_WORLD);
        	col_all_pos += ele_in_row_bloc[i];
        }

        sort(col_data, col_data + pref_sum, dataColSort);
        sort(col_all_pos, col_all_pos + pref_sum, posColSort);

		int *ele_in_col = (int *)malloc(ite_clen*sizeof(int));
		for(int i=0;i<pref_sum;i++){
			int temp = col_all_pos[i].r;
			ele_in_col[temp - ite_cstr] += 1;
		}

		int pre_sum = 0;
		for(int i=0;i<ite_clen;i++){
			pre_sum += ele_in_col[i];
			ele_in_col[i] = pre_sum;
		}

        omp pragma parallel for
        for(int i=ite_cstr;i<ite_cend;i++){
            int start;
        	if(i==ite_cstr)
        		start = 0;
        	else
        		start = ele_in_col[i - ite_rstr - 1];
        	int end = ele_in_col[i - ite_rstr];

            sort(col_data + start, col_data + end, compareNodeCol);

            for(int j=start;j<end;j++){
                col_data[j].r = col_all_pos[j].r;
                col_data[j].c = col_all_pos[j].c;
            }
        }

        sort(col_data, col_data + pref_sum, dataRowSort);
        sort(col_all_pos, col_all_pos + pref_sum, posRowSort);
		
		int *row_bloc_pref = (int *)malloc(size*sizeof(int));
    	pref_sum = 0;
    	for(int i=0;i<size;i++){
    		int temp = ele_in_row_bloc[i];
    		row_bloc_pref[i] = pref_sum;
    		pref_sum += temp;
    	}

		for(int i=0;i<size;i++){
        	MPI_Scatterv(col_data, ele_in_row_bloc, row_bloc_pref, MPI_data, data, ele_in_col_bloc[i], MPI_data, i, MPI_COMM_WORLD);
        	data += ele_in_col_bloc[i];
        	MPI_Scatterv(col_all_pos, ele_in_row_bloc, row_bloc_pref, MPI_pos, all_pos, ele_in_col_bloc[i], MPI_pos, i, MPI_COMM_WORLD);
        	all_pos += ele_in_col_bloc[i];
        }


        sort(data, data + filled_size, dataRowSort);
        sort(all_pos, all_pos + filled_size, posRowSort);

	    int *ele_in_row = (int *)malloc(ite_rlen*sizeof(int));
	    for(int i=0;i<filled_size;i++){
	    	int temp = all_pos[i].r;
			ele_in_row[temp - ite_rstr] += 1;
		}

	    int pre_sum = 0;
	    for(int i=0;i<ite_rlen;i++){
	    	pre_sum += ele_in_row[i];
	    	ele_in_row[i] = pre_sum;
	    }

        ite += 1;
        all_done = 1;
    }
    MPI_Finalize();
    return 0;
}

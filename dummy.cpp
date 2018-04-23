r, c, f, s
all_ele

omp parallel for
for(int i=0;i<n;i++){
	vector <pair <int, int> > glb_pos;
	vector <int> sort_val;
	for(int j=0;j<all_ele.length;j++){
		if(all_ele[j][0]==i){
			glb_pos.push_back(make_pair(j, all_ele[j][1]));
			sort_val.push_back(all_ele[j][2]);
		}
	}
	sort(glb_pos, my_comparator);
	sort(sort_val);
	for(int j=0;j<glb_pos.length;j++){
		all_ele[glb_pos[j][0]][2] = sort_val[j];
	}
}
# normalize counts
for x in `ls *.counts`; do python ~/box/filter_otu_table.py -i $x --norm -o ${x%.*}.norm; done

# take log
for x in `ls *.norm`; do python ~/box/filter_otu_table.py -i $x --log -o ${x%.*}.log; done

# get cutoffs
for x in `ls *.log`; do for y in 25 50 75; do python /home/csmillie/box/filter_otu_table.py -i $x --locut -20 --min_above_locut .$y -o $x.cut$y; done; done
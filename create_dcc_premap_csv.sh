SEQTYPE=${1:-"rnaseq"}
DATA_FOF="data_files_${SEQTYPE}.fof.csv"
echo "dcc_file_base,dir,original_file_base,second_file_base,final_sample_name,rand_subject_id,flag_meanings" >> ${DATA_FOF}
find $SEQTYPE -type f -name "*.fast*" \
    | sed -e "s;\([A-Za-z0-9/]*\)/\(.*\);,\1,\2,,,,+0120 = file name changed for data release to public;" \
    | tee -a ${DATA_FOF}

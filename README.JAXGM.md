# README for JAX-GM #

### Prerequisites: ###

* *List of all subjects and visits (from Stanford)
* Master sample list spreadsheet (for sample metadata and base to work from for other nodes)
* SampleSheet.csv from every sequencer run (for indexes to use with dnaPrep nodes)
* JAXid database (to link from library IDs in sample names to ID of received sample)
* QC of every run for every sample (was it analyzed, is it good enough for submission?)
* List of all files, raw fastq, clean fast[aq]


### First Steps ###

* coalesce lists of subjects, visits, samples, library indexes and all metadata
    * All subjects' metadata is supplied by Stanford, reformat it into a csv file to be used for submission.
    * Do the same for visits.

    * Corral Samples metadata and filenames; this is done using several scripts.
    1. update_master_sample_tracking_sheet
    1. merge_samples_jaxids_preps into new spreadsheet files
        *
    1. dcc_submission_file_name_change
        * rename all raw/cleaned/etc files to match the defined name structure
          "ProcectCODE_Joriginal_Jlibrary_Tn_Bn_Flags_SeqType_TissueSource_CollabID_RunID"
    1. hmp2_replace_subject_ids





    1. create mapping file of sample file name, dna prep_id and dcc_file_base; check its validity, no repeated rows, etc.
    1. run the dcc_submission_file_name_change script using that map file; this will create a checksums file holding a line for each file name, type, size and checksum values


### Submitting the data to OSDF ###
* Create python api for interaction with the OSDF api 'cutlass'.
* Run the main script for each level of the node hierarchy, a piece at a time, debugging my api,
  their cutlass api, testing and verifying the submitted data.


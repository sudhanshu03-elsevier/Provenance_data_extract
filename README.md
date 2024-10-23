# Provenance_data_extract
Extracting the provenance data from the rnf2 files.

This scripts extracts the version numbers of a libraries/packages that have changed from last test run to the latest test run. 

The script also allows you to select the buckets of interest for the test set and comparator (please choose those related to "BCE CAR FLOW" testruns only). You have the flexibility to select a UAT environment test run for your test set and an INT environment test run for your comparator, or vice versa. Alternatively, you may choose the same environment for both your test set and comparator.

This script reads and extracts provenance data from RNF2 files present in the S3 bucket. It is specifically designed to read the "BCE CAR FLOW" test runs, which must follow the path structure in the S3 bucket """<testrun>/<orderid+carId>/<intermediate_response>/<service>/<filename>""". Please ensure that this path structure is maintained.

If you have any RNF2 files on your local system and would like to extract the provenance data, please place them in the "Input_files" folder. The script will extract the data along with the test set and comparator comparison. If you do not have any such files, you may leave this folder empty and simply run the script. Please note that the script currently only reads the first file present in Input_files folder.


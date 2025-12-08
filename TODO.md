# TODO-001

The goal of this project is to create a python script that :

* Given two different folder paths, will compare both folders contents and merge them in a third new separated folder
* The goal is to merge folder that really look alike but have small differences, or sometimes conflicting files with the same name but different update date of contents
* The merge is a full join in that if a file/folder is not present in the both root folders, then we keep it in the third one 
* Conflicts should be handled this way:
  * Suggest the user directly on the CLI to 
    1: Prefer more recent
    2: Prefer least recent 
    3: Open both files to decide (and then offer option 1 or 2 again)
  * The user should just give a number between 1 and 3 and press enter

Other features of the script :

* Being able to handle very large folders that would take hours to compare, meaning if the script fails, and we need to run it again, we need to save the intermediate state to start again at where we were
* The script should offer a visual CLI progression status
* The comparison of files have to use a fast hashing method and compare hashes
* The conflicting files, their attributes (hash, last_modified_date), and the decision if any, taken by the cli user, have to be logged in a dedicated log file


Added information :

* The two folders won't change until the script has totally completed its job 

# TODO-002 

* The conflicts handling should not block the comparison/hashing of other files in the meanwhile


# TODO-003

* Fully test the program (100% code/branch coverage)


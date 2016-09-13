## How to contribute 

### Creating the fork of the HydraR

1) fork the master https://github.com/SparkTC/spark-hydrar at my own repo https://github.com/aloknsingh/spark-hydrar
2) created the git clone as per https://github.com/aloknsingh/spark-hydrar#hydrar-development-and_usages
3) add the upstream command as follows
  ```
    git remote  add upstream https://github.com/SparkTC/spark-hydrar
  ```
  
### Code Devopment in the laptop 

1) lets say you decide decide to work on the jira says REQS-117 so create the branch
2) git checkout -b <USER_ID>_REQS-117<_OPTIONAL_ANY_STRING> i.e git checkout -b aloknsingh_REQS-117
3) I do my development as needed and checkin in the local repo

  ```
  git add <new_files>
  ./bin/install-all.sh | tee install.log
  egrep -i "error" install.log
  git commit
  ```

### Before pushing your changes to upstream.

1) Now we are ready to create the PR. make sure your branch is unto date with the latest in the upstream

```
git pull --rebase upstream master
git status # run the status
```

if (merge conflicts) { # merge conflicts will be with << and >>
 look the pattern <<  and >> and fix merge conflicts manually using editor

```
git add -A # add the code}
git rebase --continue # finish the rebase process after adding fixing conflicts
```

### Test the code before pushing to upstream

```
#run the test case
./bin/install-all.sh | tee install.log
egrep -i "error" install.log
```

### Push to upstream branch

```
git push --set-upstream origin aloknsingh_REQS-117
```

### To keep your repo  also upto date with the latest changes

```
git push
```

### Create the Pull Request 
Go to your own repo at  https://github.com/aloknsingh/spark-hydrar and create the PR

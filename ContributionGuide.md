## How to contribute 

### Create a fork of R4ML

1) Fork the master https://github.com/CODAIT/R4ML to your own repo (ex: https://github.com/aloknsingh/R4ML)

2) Clone the resipatory using git clone. (See https://github.com/CODAIT/R4ML#r4ml-development-and-usages)

3) Add the upstream
  ```
    git remote add upstream https://github.com/CODAIT/R4ML
  ```
  
### Local development 

1) Create a branch for the JIRA you are working on
```
git checkout -b <GITHUB_USER_ID>_<JIRA_ID><OPTIONAL_STRING>
```
For example:
```
git checkout -b aloknsingh_R4ML-117
```
2) Make the necessary changes to the source code

3) Check-in the files you modified

  ```
  git add <MODIFIED_FILES>
  ```

4) Test your changes
  ```
  ./bin/install-all.sh | tee install.log
  egrep -i "error" install.log
  ```
5) Resolve any errors and re-run the test again as needed. Once everything looks good commit your changes to your local branch.
  ```
  git commit
  ```

### Merge your branch

1) Make sure your branch is upto date

```
git pull --rebase upstream master
git status
```

2) If there are any merge conflicts fix them manually by editing the source code (hint: look for "<<<<<<<" and ">>>>>>>"). Once you have resolved the conflicts re-add the files using `git add -A`

3) Once you are done fixing conflicts run the following:
```
git rebase --continue
```

### Test the code before pushing to upstream

```
./bin/install-all.sh | tee install.log
egrep -i "error" install.log
```

### If everything looks good push to upstream branch

```
git push --set-upstream origin <GITHUB_USER_ID>_<JIRA_ID>
# in case of above command not working
# you can try using the force options
# git push -f --set-upstream origin <GITHUB_USER_ID>_<JIRA_ID>
# it is recommented you merge the changes before using the -f but above options shuold
# be fine
```

### Update your local repository

```
git push
```

### Create a Pull Request
1) Login to github.com, go to your local branch, select the branch you worked on and click "Compare & pull request". 

2) Make sure everything looks good then click "Create pull request"


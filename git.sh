git config --global user.name "Timothy Spann"
git config --global user.email "tspann@hortonworks.com"
git add --all
git commit -m "Updated"
git push -u origin master
git remote add origin git@github.com:tspannhw/nifi-convertjsontoddl-processor.git
git commit -am "Init"

git remote -v
git push origin master

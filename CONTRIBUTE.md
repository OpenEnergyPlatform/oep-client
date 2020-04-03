```
# commit
black .
bumpversion --allow-dirty minor
git add .
git status
git commit
git push

# deploy
python setup.py sdist
twine upload dist/*
```
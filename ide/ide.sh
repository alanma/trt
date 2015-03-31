if [ ! -d "env" ]; then
    virtualenv env
fi
source env/bin/activate && pip install -r requirements.txt > /dev/null
source env/bin/activate && python ide.py
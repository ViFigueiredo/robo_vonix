python -m venv .venv
python3 -m venv .venv

.venv\Scripts\activate
source .venv/bin/activate

pip install pipreqs
pipreqs . --ignore .venv

pip install -r requirements.txt

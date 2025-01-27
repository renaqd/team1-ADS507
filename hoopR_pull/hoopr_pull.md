# How to use hoopr_data_pull.py


## Step 1: Navigate to hoopR_pull folder in command line

**Option 1:** activate your environment with conda

**Option 2:** activate your virtual environment
```
.\.venv\Scripts\activate

```



## Step 2: Enter MySQL db connection credentials 

* Modify template.env file and Enter user, pw, and host
* Rename rename "template.env" file to ".env"

## Step 3: Install dependencies

```
pip install -r requirements.txt

```

## Step 4: Run python code

**Windows**

```
python hooper_pull.py

```

**Mac**

```
python3 hooper_pull.py

```
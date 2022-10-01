echo "Creating a directory structure for the project"

directories=("src" "webapp" "webapp/img" "webapp/css" "webapp/templates" "notebooks" "application_loggings" "app_exception" "logs" "reports" "saved_models" "Unittest")
for dir in ${directories[@]}
do
    mkdir $dir
done

# Create a file
files_to_create=("src/__init__.py" "Unittest/__init__.py" "webapp/__init__.py" "webapp/templates/__init__.py" "webapp/css/__init__.py" "webapp/img/__init__.py" "app_exception/__init__.py" "application_loggings/__init__.py"  "logs/__init__.py" "app.py" "Dockerfile" "requirements.txt" "data_schema.json" "dvc.yaml" "params.yaml")
for file in ${files_to_create[@]}
do
    touch $file
done

echo "Directory structure created successfully"
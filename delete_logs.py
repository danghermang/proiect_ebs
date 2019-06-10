import os

if os.path.isdir("./logs"):
    for file in os.listdir("./logs"):
        os.remove(os.path.join("./logs", file))
        print("Deleted", file)
else:
    print("No logs to delete")

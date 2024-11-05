from main import app  # Make sure to import your Flask app correctly

# The Gunicorn server expects an 'application' callable by default.
application = app

if __name__ == "__main__":
    application.run()
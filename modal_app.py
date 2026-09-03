import modal

app = modal.App("tick-collector")

image = modal.Image.debian_slim(python_version="3.11").pip_install_from_requirements("requirements.txt")

# Secrets: create via `modal secret create arrow-secrets ARROW_APP_ID=... ARROW_APP_SECRET=... ARROW_TOTP_SECRET=... ARROW_USER_ID=... ARROW_PASSWORD=... TELEGRAM_BOT_TOKEN=... TELEGRAM_CHAT_ID=...`
# Or `modal secret create` via dashboard.

@app.function(
    image=image,
    secrets=[modal.Secret.from_name("arrow-secrets")],
    timeout=7*3600,  # 7h single session 09:10-15:40, no split
    retries=modal.Retries(max_retries=2),
    min_containers=0,
)
def collect():
    import os
    os.environ["TZ"] = "Asia/Kolkata"
    import subprocess, sys
    print("Starting Modal tick collector 09:15-15:40 IST single session")
    subprocess.run([sys.executable, "run_collect.py", "--until", "15:40"], check=False)

@app.function(image=image, schedule=modal.Cron("45 3 * * 1-5"))  # 09:15 IST = 03:45 UTC (user wants 9:15, not 9:10)
def scheduled():
    collect.remote()

if __name__ == "__main__":
    # local test: modal run modal_app.py::collect
    pass

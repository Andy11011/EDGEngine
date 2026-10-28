import asyncio
import json
import os

import requests

class Telegrammer:
    def __init__(self):
        """
        Initialize the Telegrammer.

        :param bot_token: Telegram bot token from @BotFather
        :param chat_id: Chat ID of a user or group (you can get it using /getUpdates or via another bot)
        """
        # Env vars first (containers), telegrammer.json as local fallback.
        token = os.getenv("TELEGRAM_BOT_TOKEN")
        chat_id = os.getenv("TELEGRAM_CHAT_ID")
        if not (token and chat_id):
            with open("telegrammer.json") as f:
                config = json.load(f)
            token, chat_id = config["BOT_TOKEN"], config["CHAT_ID"]

        self.bot_token = token
        self.chat_id = chat_id
        self.api_url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"

    def send_message(self, text: str, parse_mode: str = "HTML", disable_notification: bool = False) -> bool:
        """
        Send a message to the Telegram chat.

        :param text: Message text
        :param parse_mode: Text formatting mode: 'Markdown', 'HTML', or None
        :param disable_notification: If True, the message will be sent silently (no notification sound)
        :return: True if the message was sent successfully, False otherwise
        """
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": parse_mode,
            "disable_notification": disable_notification
        }

        try:
            response = requests.post(self.api_url, data=payload, timeout=10)
            response.raise_for_status()
            return True
        except requests.exceptions.RequestException as e:
            print(f"[Telegrammer] Error sending message: {e}")
            return False

    async def send_message_async(self, text: str, parse_mode: str = "HTML", disable_notification: bool = False) -> bool:
        """Non-blocking wrapper: runs the blocking request in a thread so the
        trading event loop is never stalled by Telegram latency."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None, lambda: self.send_message(text, parse_mode, disable_notification)
        )


if __name__ == "__main__":

    tg = Telegrammer()

    message = "✅ Test message from the Telegrammer module!"
    success = tg.send_message(message)

    if success:
        print("✅ Message was sent successfully.")
    else:
        print("❌ Failed to send the message.")



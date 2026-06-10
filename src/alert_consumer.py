import json
from confluent_kafka import Consumer
from src.kafka_config import KAFKA_BROKER, TOPIC_ALERTS

RED = "\033[91m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
BOLD = "\033[1m"
DIM = "\033[2m"
RESET = "\033[0m"


REASON_LABELS = {
    "valor_alto": "valor alto",
    "transacoes_rapidas": "transacoes rapidas",
    "deslocamento_impossivel": "deslocamento impossivel",
    "modelo_suspeito": "modelo suspeito",
}


def format_alert(alert):
    is_fraud = alert.get("is_fraud")
    confidence = alert.get("confidence", 0)
    amount = alert.get("amount", 0)

    if is_fraud:
        header = f"{RED}{BOLD}  FRAUDE{RESET}"
    else:
        header = f"{GREEN}{BOLD}  OK{RESET}"

    reasons = alert.get("reasons", [])
    if reasons:
        labels = [REASON_LABELS.get(r, r) for r in reasons]
        reasons_str = ", ".join(labels)
    else:
        reasons_str = ""

    card = alert.get("card_id", "?")
    card_type = alert.get("card_type", "?")
    card_brand = alert.get("card_brand", "?")
    ts = alert.get("timestamp", "?")

    lines = [
        f"{header}  {CYAN}{card}{RESET}  {DIM}{card_type} {card_brand}{RESET}",
        f"         R$ {amount:>9,.2f}   conf: {confidence:.0%}   {reasons_str}",
        f"         {DIM}{ts}{RESET}",
    ]
    return "\n".join(lines)


def run_alert_consumer():
    conf = {
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": "fraud-alert-display",
        "auto.offset.reset": "latest",
    }
    consumer = Consumer(conf)
    consumer.subscribe([TOPIC_ALERTS])

    print(f"{YELLOW}Consumer de alertas iniciado. Monitorando {TOPIC_ALERTS}...{RESET}")
    print()

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Erro: {msg.error()}")
                continue

            try:
                alert = json.loads(msg.value().decode("utf-8"))
                if "error" in alert:
                    print(f"{RED}[ERRO]{RESET} {alert.get('error', 'desconhecido')}")
                    continue
                print(format_alert(alert))
            except json.JSONDecodeError:
                print(f"Mensagem invalida: {msg.value()}")
    except KeyboardInterrupt:
        print("Consumer encerrado.")
    finally:
        consumer.close()


if __name__ == "__main__":
    run_alert_consumer()
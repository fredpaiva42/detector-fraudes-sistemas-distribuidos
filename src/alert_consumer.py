import json
from confluent_kafka import Consumer
from src.kafka_config import KAFKA_BROKER, TOPIC_ALERTS

GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
RESET = "\033[0m"
BOLD = "\033[1m"


def format_alert(alert):
    if alert.get("is_fraud"):
        color = RED
        status = "FRAUDE"
    else:
        color = GREEN
        status = "APROVADO"

    confidence = alert.get("confidence", 0)
    reasons = ", ".join(alert.get("reasons", []))

    return (
        f"{color}{BOLD}[{status}]{RESET} "
        f"Card: {alert.get('card_id', '?')} | "
        f"Tipo: {alert.get('card_type', '?')} | "
        f"Bandeira: {alert.get('card_brand', '?')} | "
        f"Valor: R$ {alert.get('amount', 0):.2f} | "
        f"Confiança: {confidence:.1%} | "
        f"Motivos: {reasons or 'nenhum'} | "
        f"Timestamp: {alert.get('timestamp', '?')}"
    )


def run_alert_consumer():
    conf = {
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": "fraud-alert-display",
        "auto.offset.reset": "latest",
    }
    consumer = Consumer(conf)
    consumer.subscribe([TOPIC_ALERTS])

    print(f"{YELLOW}Consumer de alertas iniciado. Monitorando {TOPIC_ALERTS}...{RESET}")
    print("-" * 100)

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
                print(f"Mensagem inválida: {msg.value()}")
    except KeyboardInterrupt:
        print("Consumer encerrado.")
    finally:
        consumer.close()


if __name__ == "__main__":
    run_alert_consumer()

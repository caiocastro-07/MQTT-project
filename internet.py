import json
import random
import time
import sys

import paho.mqtt.client as mqtt


# -------------------------------------------------------------------------
# Configurações gerais
# -------------------------------------------------------------------------
BROKER_HOST = "broker.emqx.io"
BROKER_PORT = 1883
KEEPALIVE   = 60                # segundos
TOPICO = 'predio/sala/1/sensores'
SALAS = ["sala_1", "sala_2", "sala_3"]
PUBLISH_INTERVAL = 5            # segundos

# -------------------------------------------------------------------------
# Callbacks MQTT
# -------------------------------------------------------------------------
def on_connect(client, userdata, flags, rc, properties=None):
    """Chamado quando a conexão TCP + handshake MQTT são concluídos."""
    if rc == 0:
        print("[Publisher] ✅ Conectado ao broker MQTT")
    else:
        print(f"[Publisher] ❌ Falha na conexão (rc={rc})")
        sys.exit(1)


def on_disconnect(client, userdata, rc, properties=None):
    """Útil para debug – o loop_forever tentará reconectar automaticamente."""
    print(f"[Publisher] 🔌 Desconectado (rc={rc})")


# -------------------------------------------------------------------------
# Função auxiliar – gera valores aleatórios dentro dos intervalos pedidos
# -------------------------------------------------------------------------
def gerar_medicao():
    """
    Retorna uma tupla (temperatura, umidade) com:
        - temperatura  ∈ [18.0 , 35.0]  °C
        - umidade      ∈ [30.0 , 80.0]  %
    """
    temperatura = round(random.uniform(18.0, 35.0), 1)
    umidade     = round(random.uniform(30.0, 80.0), 1)
    return temperatura, umidade


# -------------------------------------------------------------------------
# Main
# -------------------------------------------------------------------------
def main():
    # 1️⃣ Cria cliente MQTT (client_id aleatório para evitar colisões)
    client = mqtt.Client(client_id=f"publisher_{random.randint(1_000, 9_999)}",
                         protocol=mqtt.MQTTv311)

    # 2️⃣ Anexa callbacks
    client.on_connect    = on_connect
    client.on_disconnect = on_disconnect

    # 3️⃣ Conecta ao broker público (sem TLS, sem auth)
    try:
        client.connect(BROKER_HOST, BROKER_PORT, KEEPALIVE)
    except Exception as exc:
        print(f"[Publisher] ❌ Erro ao conectar: {exc}")
        sys.exit(1)

    # 4️⃣ Inicia loop de rede em background (non‑blocking)
    client.loop_start()

    try:
        while True:
            for sala in SALAS:
                temp, umid = gerar_medicao()
                payload = {
                    "sala_id"   : sala,
                    "temperatura": temp,
                    "umidade"    : umid
                }
                topic = f"predio/sala/{sala}/sensores"
                # QoS 1 → entrega garantida (pelo menos uma vez)
                result = client.publish(topic, json.dumps(payload), qos=1)

                # result.is_published() é True apenas quando QoS 0; para QoS 1/2
                # podemos checar result.rc == mqtt.MQTT_ERR_SUCCESS
                if result.rc == mqtt.MQTT_ERR_SUCCESS:
                    print(f"[Publisher] 📤 {topic} → {payload}")
                else:
                    print(f"[Publisher] ⚠️ Falha ao publicar em {topic} (rc={result.rc})")

            # Aguarda o intervalo configurado
            time.sleep(PUBLISH_INTERVAL)

    except KeyboardInterrupt:
        print("\n[Publisher] 🛑 Interrompido pelo usuário (Ctrl+C)")

    finally:
        client.loop_stop()
        client.disconnect()
        print("[Publisher] 👋 Desconectado e finalizado.")


if __name__ == "__main__":
    main()
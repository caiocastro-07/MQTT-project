import json
import sys

import paho.mqtt.client as mqtt


# -------------------------------------------------------------------------
# Configurações
# -------------------------------------------------------------------------
BROKER_HOST = "broker.emqx.io"
BROKER_PORT = 1883
KEEPALIVE   = 60
PREDIO = 'predio/sala/+/sensores'

# Tópico de telemetria (wildcard + = “qualquer sala”)
TOPIC_TELEMETRIA = "predio/sala/+/sensores"
# Prefixo do tópico de comando – o ID da sala será anexado em runtime
TOPIC_COMANDO_PREFIX = "predio/sala/"

# Regras de decisão (constantes – fáceis de ajustar)
TEMP_LIGAR = 27.0
UMID_LIGAR = 55.0

# -------------------------------------------------------------------------
# Estado interno – dicionário {sala_id: "LIGADO"|"DESLIGADO"}
# -------------------------------------------------------------------------
estado_ar = {}   # será preenchido dinamicamente conforme mensagens chegam


# -------------------------------------------------------------------------
# Callbacks MQTT
# -------------------------------------------------------------------------
def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print("[Subscriber] ✅ Conectado ao broker")
        # Inscreve no wildcard – QoS 1 para não perder mensagens
        client.subscribe(TOPIC_TELEMETRIA, qos=1)
        print(f"[Subscriber] 📥 Inscrito em '{TOPIC_TELEMETRIA}'")
    else:
        print(f"[Subscriber] ❌ Falha na conexão (rc={rc})")
        sys.exit(1)


def on_disconnect(client, userdata, rc, properties=None):
    print(f"[Subscriber] 🔌 Desconectado (rc={rc})")
    # O loop_forever fará reconexão automática (paho ≥1.5)


def on_message(client, userdata, msg):
    """
    Processa cada payload de telemetria.
    Tópico esperado: predio/sala/<ID>/sensores
    """
    try:
        payload = json.loads(msg.payload.decode())
    except json.JSONDecodeError:
        print(f"[Subscriber] ⚠️ Payload inválido (não‑JSON) em {msg.topic}")
        return

    # Extrai ID da sala a partir do tópico (formato fixo)
    # Ex.: "predio/sala/sala_2/sensores" → parts[2] == "sala_2"
    parts = msg.topic.split("/")
    if len(parts) != 4 or parts[0] != "predio" or parts[1] != "sala" or parts[3] != "sensores":
        print(f"[Subscriber] ⚠️ Tópico inesperado: {msg.topic}")
        return
    sala_id = parts[2]

    # Valida campos obrigatórios
    temperatura = payload.get("temperatura")
    umidade     = payload.get("umidade")
    if temperatura is None or umidade is None:
        print(f"[Subscriber] ⚠️ Campos ausentes no payload: {payload}")
        return

    # -------------------------------------------------------------
    # Regra de decisão
    # -------------------------------------------------------------
    ligar = (temperatura > TEMP_LIGAR) or (umidade > UMID_LIGAR)
    novo_estado = "LIGAR" if ligar else "DESLIGAR"

    # -------------------------------------------------------------
    # Publica apenas se houver mudança de estado
    # -------------------------------------------------------------
    estado_anterior = estado_ar.get(sala_id)
    if novo_estado != estado_anterior:
        estado_ar[sala_id] = novo_estado
        topico_comando = f"{TOPIC_COMANDO_PREFIX}{sala_id}/ar_condicionado"
        cmd_payload = json.dumps({"acao": novo_estado})
        result = client.publish(topico_comando, cmd_payload, qos=1, retain=True)
        if result.rc == mqtt.MQTT_ERR_SUCCESS:
            print(f"[Subscriber] 🎛️ {sala_id} → {novo_estado}  (t={temperatura}°C, u={umidade}%)")
        else:
            print(f"[Subscriber] ⚠️ Falha ao publicar comando para {sala_id} (rc={result.rc})")
    else:
        # Estado inalterado – apenas log opcional (comentado para não poluir)
        # print(f"[Subscriber] ℹ️ {sala_id} mantém {novo_estado}")
        pass


# -------------------------------------------------------------------------
# Main
# -------------------------------------------------------------------------
def main():
    client = mqtt.Client(client_id=f"subscriber_{__import__('random').randint(1_000,9_999)}",
                         protocol=mqtt.MQTTv311)

    client.on_connect    = on_connect
    client.on_disconnect = on_disconnect
    client.on_message    = on_message

    # Conexão (retry automático dentro de loop_forever)
    try:
        client.connect(BROKER_HOST, BROKER_PORT, KEEPALIVE)
    except Exception as exc:
        print(f"[Subscriber] ❌ Erro ao conectar: {exc}")
        sys.exit(1)

    print("[Subscriber] 🔄 Iniciando loop infinito (Ctrl+C para sair)…")
    try:
        client.loop_forever()          # bloqueante, trata reconexão sozinho
    except KeyboardInterrupt:
        print("\n[Subscriber] 🛑 Interrompido pelo usuário")
    finally:
        client.disconnect()
        print("[Subscriber] 👋 Desconectado.")


if __name__ == "__main__":
    main()

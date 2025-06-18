import os
import time
import logging
from kubernetes import client, config
from kubernetes.client.rest import ApiException
import requests
from collections import defaultdict

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()  # Вывод в консоль
    ]
)
logger = logging.getLogger(__name__)

# Пороговые значения
CPU_THRESHOLD = float(os.getenv("CPU_THRESHOLD", "90.0"))  # %
MEM_THRESHOLD = float(os.getenv("MEM_THRESHOLD", "90.0"))  # %
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "180"))   # секунд между проверками
MAX_CORDON_NODES = int(os.getenv("MAX_CORDON_NODES", "3")) # Максимальное количество нод в состоянии CORDON
REQUIRED_SUCCESS_CHECKS = int(os.getenv("REQUIRED_SUCCESS_CHECKS", "3")) # Количество проверок перед CORDON

# Количества удачных проверок для каждой нод
success_checks = defaultdict(int)

# === Автоматическое определение типа запуска ===
def load_kube_config_auto():
    try:
        if os.getenv('KUBERNETES_SERVICE_HOST'):
            # Запуск внутри Pod в Kubernetes
            config.load_incluster_config()
            print("[✔] Используется in-cluster config")
        else:
            # Локальный запуск
            kubeconfig_path = os.path.expanduser("~/.kube/config")
            if os.path.exists(kubeconfig_path):
                config.load_kube_config(config_file=kubeconfig_path)
                print(f"[✔] Используется локальный kubeconfig: {kubeconfig_path}")
            else:
                raise Exception("Локальный kubeconfig не найден")
    except Exception as e:
        print(f"[✘] Ошибка загрузки конфигурации Kubernetes: {e}")
        exit(1)

load_kube_config_auto()
core_v1 = client.CoreV1Api()
metrics_api = client.CustomObjectsApi()

# === Telegram уведомления ===
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
YM_TOKEN = os.getenv("YM_TOKEN")
YM_CHAT_ID = os.getenv("YM_CHAT_ID")
YANDEX_MESSENGER_API_URL = os.getenv("YANDEX_MESSENGER_API_URL")

def send_telegram_message(message, parse_mode="HTML"):
    if TELEGRAM_TOKEN and TELEGRAM_CHAT_ID:
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": parse_mode}
            resp = requests.post(url, json=payload)
            if resp.status_code != 200:
                print(f"[✘] Ошибка отправки в Telegram: {resp.text}")
        except Exception as e:
            print(f"[✘] Telegram error: {e}")
    else:
        print("[!] Telegram уведомления не настроены (переменные окружения не заданы)")

def send_message_to_yandex(chat_id, text):
    """Отправляем сообщение в Яндекс.Мессенджер"""
    headers = {
        "Authorization": f"OAuth {YM_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {
        "chat_id": chat_id,
        "text": text
    }
    response = requests.post(YANDEX_MESSENGER_API_URL, json=payload, headers=headers)
    if response.status_code == 200:
        logging.info("Сообщение отправлено в Яндекс.Мессенджер")
    else:
        logging.error(f"Ошибка отправки: {response.text}")

def send_alerts(message):
    send_telegram_message(message, parse_mode="HTML")
    yandex_message = message.replace("<b>", "**").replace("</b>", "**")
    send_message_to_yandex(YM_CHAT_ID, yandex_message)

def get_node_metrics():
    # Получаем метрики через metrics.k8s.io API
    try:
        metrics = metrics_api.list_cluster_custom_object(
            group="metrics.k8s.io",
            version="v1beta1",
            plural="nodes"
        )
        # Фильтруем master-ноды
        filtered_metrics = []
        for item in metrics['items']:
            node_name = item['metadata']['name']
            node = core_v1.read_node(node_name)
            if not is_master_node(node):
                filtered_metrics.append(item)
        return {'items': filtered_metrics}
    except Exception as e:
        print(f"Ошибка получения метрик: {e}")
        return None

def get_node_capacity(node):
    cpu = node.status.capacity['cpu']
    mem = node.status.capacity['memory']
    return int(cpu), parse_memory(mem)

def parse_cpu(cpu_str):
    if cpu_str.endswith("n"):
    # CPU в nanocores  (например, 1_000_000_000n = 1 core)
        return int(cpu_str[:-1]) / 1_000_000_000
    elif cpu_str.endswith("u"):
    # CPU в microcores (например, 1_000_000u = 1 core)
        return int(cpu_str[:-1]) / 1_000_000        
    # CPU в millicores (например, 1000m = 1 core)
    elif cpu_str.endswith("m"):
        return int(cpu_str[:-1]) / 1000
    else:
        return float(cpu_str)

def parse_memory(mem_str):
    # Преобразуем память в MiB
    if mem_str.endswith("Ki"):
        return int(mem_str[:-2]) / 1024
    elif mem_str.endswith("Mi"):
        return int(mem_str[:-2])
    elif mem_str.endswith("Gi"):
        return int(mem_str[:-2]) * 1024
    return int(mem_str)

def cordon_node(node_name):
    try:
        body = {"spec": {"unschedulable": True}}
        core_v1.patch_node(node_name, body)
        msg = (
            f"⚠️ <b>ALARM</b>\n"
            f"Нода {node_name} перегружена\n"
            f"Нода поставлена в CORDON"
        )
        logger.info(msg)
        send_alerts(msg)
        # Сбрасываем счетчик успешных проверок при CORDON
        success_checks.pop(node_name, None)
    except Exception as e:
        logger.error(f"Ошибка при установке CORDON для ноды {node_name}: {e}")

def uncordon_node(node_name):
    try:
        body = {"spec": {"unschedulable": False}}
        core_v1.patch_node(node_name, body)
        msg = (
            f"✅  <b>RESOLVED</b>\n"
            f"Нагрузка на ноде {node_name} нормализовалась после {REQUIRED_SUCCESS_CHECKS} проверок\n"
            f"UNCORDON выполнен"
        )
        logger.info(msg)
        send_alerts(msg)
        # Сбрасываем счетчик после успешного UNCORDON
        success_checks.pop(node_name, None)
    except Exception as e:
        logger.error(f"Ошибка при снятии CORDON с ноды {node_name}: {e}")

def is_node_cordoned(node):
    return node.spec.unschedulable

def is_master_node(node):
    """
    Проверяет, является ли нода master-нодой.
    Master-ноды обычно имеют метку node-role.kubernetes.io/control-plane или node-role.kubernetes.io/master.
    """
    labels = node.metadata.labels
    return (
        "node-role.kubernetes.io/control-plane" in labels
        or "node-role.kubernetes.io/master" in labels
    )

def monitor():
    logger.info("Запуск мониторинга нод Kubernetes")
    logger.info(f"Параметры: CPU_THRESHOLD={CPU_THRESHOLD}%, MEM_THRESHOLD={MEM_THRESHOLD}%")
    logger.info(f"CHECK_INTERVAL={CHECK_INTERVAL}s, MAX_CORDON_NODES={MAX_CORDON_NODES}")
    logger.info(f"REQUIRED_SUCCESS_CHECKS={REQUIRED_SUCCESS_CHECKS}")

    while True:
        try:
            logger.info("Начало цикла проверки")
            metrics = get_node_metrics()
            if not metrics:
                logger.warning("Не удалось получить метрики нод")
                time.sleep(CHECK_INTERVAL)
                continue

            nodes_with_load = []
            for item in metrics['items']:
                node_name = item['metadata']['name']
                usage_cpu = parse_cpu(item['usage']['cpu'])
                usage_mem = parse_memory(item['usage']['memory'])
                node = core_v1.read_node(node_name)
                capacity_cpu, capacity_mem = get_node_capacity(node)

                cpu_percent = (usage_cpu / capacity_cpu) * 100
                mem_percent = (usage_mem / capacity_mem) * 100

                nodes_with_load.append({
                    'name': node_name,
                    'cpu_percent': cpu_percent,
                    'mem_percent': mem_percent,
                    'node': node
                })

                logger.info(f"Node: {node_name} | CPU: {cpu_percent:.1f}% | MEM: {mem_percent:.1f}%")

            nodes_with_load.sort(key=lambda x: max(x['cpu_percent'], x['mem_percent']), reverse=True)
            cordon_count = sum(1 for node_info in nodes_with_load if is_node_cordoned(node_info['node']))
            logger.info(f"Текущее количество нод в CORDON: {cordon_count}/{MAX_CORDON_NODES}")

            for node_info in nodes_with_load:
                node_name = node_info['name']
                cpu_percent = node_info['cpu_percent']
                mem_percent = node_info['mem_percent']
                node = node_info['node']

                # Проверяем превышение порогов
                if cpu_percent > CPU_THRESHOLD or mem_percent > MEM_THRESHOLD:
                    # Если нода не в CORDON и есть свободные слоты - ставим в CORDON
                    if not is_node_cordoned(node) and cordon_count < MAX_CORDON_NODES:
                        logger.warning(f"Нода {node_name} превысила порог (CPU: {cpu_percent:.1f}%, MEM: {mem_percent:.1f}%)")
                        cordon_node(node_name)
                        cordon_count += 1
                    # Сбрасываем счетчик успешных проверок при превышении порога
                    if node_name in success_checks:
                        logger.info(f"Нагрузка на ноде {node_name} снова превысила порог, сбрасываем счетчик успешных проверок")
                        success_checks.pop(node_name)
                else:
                    # Если нагрузка в норме и нода в CORDON
                    if is_node_cordoned(node):
                        success_checks[node_name] = success_checks.get(node_name, 0) + 1
                        logger.info(
                            f"Нода {node_name} в норме (CPU: {cpu_percent:.1f}%, MEM: {mem_percent:.1f}%) "
                            f"(успешных проверок: {success_checks[node_name]}/{REQUIRED_SUCCESS_CHECKS})"
                        )
                        
                        if success_checks[node_name] >= REQUIRED_SUCCESS_CHECKS:
                            logger.info(f"Нода {node_name} прошла {REQUIRED_SUCCESS_CHECKS} успешных проверок, выполняю UNCORDON")
                            uncordon_node(node_name)
                            cordon_count -= 1

            # Очищаем счетчик для нод, которых больше нет в метриках
            current_nodes = {node_info['name'] for node_info in nodes_with_load}
            for node_name in list(success_checks.keys()):
                if node_name not in current_nodes:
                    logger.info(f"Нода {node_name} больше не в метриках, удаляю из списка проверок")
                    success_checks.pop(node_name)

        except Exception as e:
            logger.error(f"Ошибка в основном цикле мониторинга: {e}", exc_info=True)
        
        logger.info(f"Ожидание {CHECK_INTERVAL} секунд до следующей проверки...")
        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    monitor()

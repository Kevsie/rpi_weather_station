from pymodbus.client import ModbusSerialClient
from influxdb import InfluxDBClient  # Local InfluxDB (v1.x)
from influxdb_client import InfluxDBClient as InfluxDBCloud, Point, WritePrecision  # Cloud InfluxDB (v2.x)
import serial.rs485
import time
from datetime import datetime
import statistics

# 🌍 InfluxDB Cloud Configuration
INFLUXDB_CLOUD_URL = "https://us-west-2-1.aws.cloud2.influxdata.com"  # Replace with your URL
INFLUXDB_CLOUD_TOKEN = "your_api_token"  # Replace with your API token
INFLUXDB_CLOUD_ORG = "No company"  # Replace with your org name
INFLUXDB_CLOUD_BUCKET = "weather_data"  # Replace with your bucket name

# Initialize InfluxDB Cloud Client
cloud_client = InfluxDBCloud(url=INFLUXDB_CLOUD_URL, token=INFLUXDB_CLOUD_TOKEN, org=INFLUXDB_CLOUD_ORG)
cloud_write_api = cloud_client.write_api(write_options=WritePrecision.NS)

# 🏠 Local InfluxDB Configuration
INFLUXDB_LOCAL_HOST = 'localhost'
INFLUXDB_LOCAL_PORT = 8086
INFLUXDB_LOCAL_DATABASE = 'weather_data'

# Initialize Local InfluxDB Client
local_client = InfluxDBClient(host=INFLUXDB_LOCAL_HOST, port=INFLUXDB_LOCAL_PORT)
local_client.create_database(INFLUXDB_LOCAL_DATABASE)
local_client.switch_database(INFLUXDB_LOCAL_DATABASE)

# ⏳ Data aggregation storage (for 15-minute cloud storage)
buffered_data = []

# ✅ Function to write data to **Local InfluxDB** (every 10 sec)
def write_to_local_influx(weather_data):
    json_body = [
        {
            "measurement": "weather",
            "tags": {"location": "home"},
            "fields": weather_data,
            "time": datetime.now(datetime.timezone.utc)
        }
    ]
    local_client.write_points(json_body)

# ✅ Function to write **aggregated data to InfluxDB Cloud** (every 15 min)
def write_to_cloud_influx():
    if not buffered_data:
        print("⚠️ No data to send to cloud!")
        return

    # Aggregate data for 15 minutes
    aggregated_data = {}
    for key in buffered_data[0].keys():
        values = [entry[key] for entry in buffered_data if entry[key] is not None]
        if values:
            aggregated_data[f"avg_{key}"] = statistics.mean(values)

    point = Point("weather")
    for key, value in aggregated_data.items():
        point = point.field(key, value)

    # Add timestamp for cloud storage
    point = point.time(datetime.now(datetime.timezone.utc), WritePrecision.NS)
    cloud_write_api.write(bucket=INFLUXDB_CLOUD_BUCKET, org=INFLUXDB_CLOUD_ORG, record=point)

    print("✅ Aggregated data sent to InfluxDB Cloud!")

    # Clear buffer after sending
    buffered_data.clear()

# ✅ Function to read sensor data
def read_weather_station_data(client):
    try:
        def read_register(address, count=1):
            result = client.read_holding_registers(address=address, count=count, slave=1)
            return result.registers if not result.isError() else None

        def read_32bit(high, low):
            return ((high << 16) + low) if high is not None and low is not None else None

        def twos_complement(value, bit_width):
            return value - (1 << bit_width) if value >= (1 << (bit_width - 1)) else value

        # Read sensor values
        temperature_raw = read_register(0x0001)
        humidity = read_register(0x0002)
        wind_speed = read_register(0x0004)
        barometric_pressure_high = read_register(0x000C)
        barometric_pressure_low = read_register(0x000D)

        # Convert values
        temperature = (twos_complement(temperature_raw[0], 16) / 10) - 40 if temperature_raw else None
        humidity = humidity[0] / 10.0 if humidity else None
        wind_speed = wind_speed[0] / 10.0 if wind_speed else None
        barometric_pressure = read_32bit(barometric_pressure_high[0], barometric_pressure_low[0]) / 100 if barometric_pressure_high and barometric_pressure_low else None

        # ✅ Create data dictionary
        data = {
            "temperature": temperature,
            "humidity": humidity,
            "wind_speed": wind_speed,
            "barometric_pressure": barometric_pressure
        }

        return data

    except Exception as e:
        print("❌ Error reading from weather station:", e)
        return {}

# ✅ Configure RS485 Serial Connection
serial_port = "/dev/ttyS0"
ser = serial.rs485.RS485(port=serial_port, baudrate=9600)
ser.rs485_mode = serial.rs485.RS485Settings(rts_level_for_tx=False, rts_level_for_rx=True, delay_before_tx=0.0, delay_before_rx=-0.0)

# ✅ Establish Modbus RTU client
client = ModbusSerialClient(method='rtu', port=serial_port, baudrate=9600)
client.socket = ser
client.connect()

# ⏳ Track time for cloud upload
last_cloud_upload_time = time.time()

try:
    while True:
        weather_data = read_weather_station_data(client)

        if weather_data:
            # ⏳ Log to local InfluxDB **every 10 seconds**
            write_to_local_influx(weather_data)
            print(f"✅ Logged locally: {weather_data}")

            # 🔄 Store data for cloud upload (buffer for 15 min)
            buffered_data.append(weather_data)

        # ⏳ Every 15 minutes, send aggregated data to InfluxDB Cloud
        if time.time() - last_cloud_upload_time >= 900:  # 900 seconds = 15 minutes
            write_to_cloud_influx()
            last_cloud_upload_time = time.time()

        time.sleep(10)  # ⏳ Collect data every 10 sec

except KeyboardInterrupt:
    print("🔴 Stopping weather station data logging.")
    client.close()

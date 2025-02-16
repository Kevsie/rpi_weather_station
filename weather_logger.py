from pymodbus.client import ModbusSerialClient
from influxdb import InfluxDBClient
from influxdb_client import InfluxDBClient, Point, WritePrecision
import serial.rs485
import time
from datetime import datetime

# Local InfluxDB setup
INFLUXDB_ADDRESS = 'localhost'
INFLUXDB_PORT = 8086
INFLUXDB_DATABASE = 'weather_data'

influx_client = InfluxDBClient(host=INFLUXDB_ADDRESS, port=INFLUXDB_PORT)
influx_client.create_database(INFLUXDB_DATABASE)
influx_client.switch_database(INFLUXDB_DATABASE)

# InfluxDB Cloud Configuration
INFLUXDB_URL = "https://us-west-2-1.aws.cloud2.influxdata.com"  # Replace with your region's URL
INFLUXDB_TOKEN = "your_api_token"  # Replace with your API token
INFLUXDB_ORG = "your_organization_name"  # Replace with your org name
INFLUXDB_BUCKET = "weather_data"  # Replace with your bucket name

# Initialize InfluxDB Client
client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
write_api = client.write_api(write_options=WritePrecision.NS)

def write_to_influx(data):
    json_body = [
        {
            "measurement": "weather",
            "tags": {
                "location": "home"
            },
            "fields": data
        }
    ]
    influx_client.write_points(json_body)

# Function to write data to influxdb cloud
def write_weather_data(weather_data):
    point = Point("weather")
    
    # Loop through dictionary and add all fields
    for key, value in weather_data.items():
        point = point.field(key, value)

    # Add timestamp
    point = point.time(datetime.utcnow(), WritePrecision.NS)

    # Write to InfluxDB
    write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)

    print("Data successfully written to InfluxDB Cloud!")

def read_weather_station_data(client):
    try:
        def read_register(address, count=1):
            result = client.read_holding_registers(address=address, count=count, slave=1)
            return result.registers if not result.isError() else None

        def read_32bit(high, low):
            if high is not None and low is not None:
                return ((high << 16) + low)
            else:
                return None

        def twos_complement(value, bit_width):
            # Convert unsigned value to signed integer using two's complement
            if value >= (1 << (bit_width - 1)):  # If value is in negative range
                value -= (1 << bit_width)  # Convert to negative
            return value

        # Read sensors
        temperature_raw = read_register(0x0001)[0]
        humidity = read_register(0x0002)[0] / 10.0 if read_register(0x0002) else None
        wind_direction = read_register(0x0003)[0] if read_register(0x0003) else None
        wind_speed = read_register(0x0004)[0] / 10.0 if read_register(0x0004) else None
        rain_intensity = read_register(0x0006)[0] / 10.0 if read_register(0x0006) else None
        max_wind_speed = read_register(0x0005)[0] / 10.0 if read_register(0x0005) else None
        rainfall_total = read_register(0x0007)[0] / 10.0 if read_register(0x0007) else None
        uv_high = read_register(0x0008)[0]
        uv_low = read_register(0x0009)[0]
        light_intensity_high = read_register(0x000A)[0]
        light_intensity_low = read_register(0x000B)[0]
        barometric_pressure_high = read_register(0x000C)[0]
        barometric_pressure_low = read_register(0x000D)[0]
        pm1_0 = read_register(0x0011)[0] if read_register(0x0011) else None
        pm2_5 = read_register(0x0012)[0] if read_register(0x0012) else None
        pm10 = read_register(0x0013)[0] if read_register(0x0013) else None
        co2 = read_register(0x0014)[0] if read_register(0x0014) else None
        noise = read_register(0x0015)[0] if read_register(0x0015) else None
        dew_point_raw = read_register(0x001A)[0]

        # Read two complement (For negative numbers)
        temperature = (twos_complement(temperature_raw, 16) / 10) - 40 if temperature_raw is not None else None
        dew_point = (twos_complement(dew_point_raw, 16) / 10) if dew_point_raw is not None else None

        # Convert 32bit values
        uv_energy = read_32bit(uv_high, uv_low)
        light_intensity = read_32bit(light_intensity_high, light_intensity_low)
        barometric_pressure = read_32bit(barometric_pressure_high, barometric_pressure_low) / 100


        # Create data dictionary
        data = {
            "temperature": temperature,
            "humidity": humidity,
            "wind_direction": wind_direction,
            "wind_speed": wind_speed,
            "rain_intensity": rain_intensity,
            "max_wind_speed": max_wind_speed,
            "rainfall_total": rainfall_total,
            "uv_energy": uv_energy,
            "barometric_pressure": barometric_pressure,
            "light_intensity": light_intensity,
            "pm1_0": pm1_0,
            "pm2_5": pm2_5,
            "pm10": pm10,
            "co2": co2,
            "noise": noise,
            "dew_point": dew_point
        }

        return data

    except Exception as e:
        print("Error reading from weather station:", e)
        return {}

# Configure RS485 - Adjust to use /dev/ttyUSB0 if needed
serial_port = "/dev/ttyS0"
ser = serial.rs485.RS485(port=serial_port, baudrate=9600)
ser.rs485_mode = serial.rs485.RS485Settings(rts_level_for_tx=False, rts_level_for_rx=True, delay_before_tx=0.0, delay_before_rx=-0.0)

# Establish Modbus RTU client
client = ModbusSerialClient(method='rtu', port=serial_port, baudrate=9600)
client.socket = ser
client.connect()

try:
    while True:
        weather_data = read_weather_station_data(client)
        
        if weather_data:
            write_to_influx(weather_data)
            print(f"Logged Weather Data: {weather_data}")
        
        time.sleep(5)  # Adjust as needed
except KeyboardInterrupt:
    print("Stopping weather station data logging.")
    client.close()


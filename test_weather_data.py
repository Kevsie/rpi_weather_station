from pymodbus.client import ModbusSerialClient

# Configure Modbus RTU
client = ModbusSerialClient(method='rtu', port='/dev/ttyS0', baudrate=9600)
client.connect()

# Read raw temperature register
result8 = client.read_holding_registers(address=0x000C, count=1, slave=1)
result9 = client.read_holding_registers(address=0x000D, count=1, slave=1)

if not result9.isError():
    raw_temp8 = result8.registers[0]
    raw_temp9 = result9.registers[0]
    pressu = ((raw_temp8 << 16) + raw_temp9)/100
    print(f"Raw Temperature Register Value: {pressu}")
else:
    print("Error reading temperature register.")

client.close()

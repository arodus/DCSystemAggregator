#!/usr/bin/env python

import os
import sys
from script_utils import SCRIPT_HOME, VERSION
sys.path.insert(1, os.path.join(os.path.dirname(__file__), f"{SCRIPT_HOME}/ext"))

import dbus
from dbus.mainloop.glib import DBusGMainLoop
from gi.repository import GLib
import logging
from vedbus import VeDbusService
from settableservice import SettableService
from dbusmonitor import DbusMonitor
from collections import namedtuple

DEVICE_INSTANCE_ID = 1024
PRODUCT_ID = 0
PRODUCT_NAME = "DC System Aggregator"
FIRMWARE_VERSION = 0
HARDWARE_VERSION = 0
CONNECTED = 1

ALARM_OK = 0
ALARM_WARNING = 1
ALARM_ALARM = 2

VOLTAGE_DEADBAND = 1.0

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("dcsystem")


class SystemBus(dbus.bus.BusConnection):
    def __new__(cls):
        return dbus.bus.BusConnection.__new__(cls, dbus.bus.BusConnection.TYPE_SYSTEM)


class SessionBus(dbus.bus.BusConnection):
    def __new__(cls):
        return dbus.bus.BusConnection.__new__(cls, dbus.bus.BusConnection.TYPE_SESSION)


def dbusConnection():
    return SessionBus() if 'DBUS_SESSION_BUS_ADDRESS' in os.environ else SystemBus()


DCService = namedtuple('DCService', ['name', 'type'])


VOLTAGE_TEXT = lambda path,value: "{:.2f}V".format(value)
CURRENT_TEXT = lambda path,value: "{:.3f}A".format(value)
POWER_TEXT = lambda path,value: "{:.2f}W".format(value)
ENERGY_TEXT = lambda path,value: "{:.6f}kWh".format(value)


class DCSystemService(SettableService):
    def __init__(self, conn):
        super().__init__()
        self.service = VeDbusService('com.victronenergy.dcsystem.aggregator', conn, register=False)
        self.add_settable_path("/CustomName", "")
        self._init_settings(conn)
        di = self.register_device_instance("dcsystem", "DCSystemAggregator", DEVICE_INSTANCE_ID)
        self.service.add_mandatory_paths(__file__, VERSION, 'dbus', di,
                                     PRODUCT_ID, PRODUCT_NAME, FIRMWARE_VERSION, HARDWARE_VERSION, CONNECTED)
        self.service.add_path("/Dc/0/Voltage", 0, gettextcallback=VOLTAGE_TEXT)
        self.service.add_path("/Dc/0/Current", 0, gettextcallback=CURRENT_TEXT)
        self.service.add_path("/History/EnergyIn", 0, gettextcallback=ENERGY_TEXT)
        self.service.add_path("/History/EnergyOut", 0, gettextcallback=ENERGY_TEXT)
        self.service.add_path("/Alarms/LowVoltage", ALARM_OK)
        self.service.add_path("/Alarms/HighVoltage", ALARM_OK)
        self.service.add_path("/Alarms/LowTemperature", ALARM_OK)
        self.service.add_path("/Alarms/HighTemperature", ALARM_OK)
        self.service.add_path("/Dc/0/Power", 0, gettextcallback=POWER_TEXT)
        self.service.register()
        self._local_values = {}
        for path in self.service._dbusobjects:
            self._local_values[path] = self.service[path]
        options = None  # currently not used afaik
        self.monitor = DbusMonitor({
            'com.victronenergy.battery': {
                '/Dc/0/Power': options,
                '/Dc/0/Voltage': options,
                '/Dc/0/Current': options,
            },
            'com.victronenergy.vebus': {
                '/Dc/0/Power': options,
                '/Dc/0/Voltage': options,
                '/Dc/0/Current': options,
                '/Ac/Out/L1/P': options,
                '/Ac/Out/L2/P': options,
                '/Ac/Out/L3/P': options,
            },
            'com.victronenergy.solarcharger': {
                '/Dc/0/Voltage': options,
                '/Dc/0/Current': options,
                '/Load/I': options,
            },
            'com.victronenergy.charger': {
                '/Dc/0/Voltage': options,
                '/Dc/0/Current': options,
            },
            'com.victronenergy.fuelcell': {
                '/Dc/0/Voltage': options,
                '/Dc/0/Current': options,
            },
            'com.victronenergy.alternator': {
                '/Dc/0/Power': options,
            },
            'com.victronenergy.dcload': {
                '/Dc/0/Power': options,
                '/Dc/0/Voltage': options,
                '/Dc/0/Current': options,
                '/History/EnergyIn': options,
                '/Alarms/LowVoltage': options,
                '/Alarms/HighVoltage': options,
                '/Alarms/LowTemperature': options,
                '/Alarms/HighTemperature': options,
            },
            'com.victronenergy.dcsource': {
                '/Dc/0/Power': options,
                '/Dc/0/Voltage': options,
                '/Dc/0/Current': options,
                '/History/EnergyOut': options,
                '/Alarms/LowVoltage': options,
                '/Alarms/HighVoltage': options,
                '/Alarms/LowTemperature': options,
                '/Alarms/HighTemperature': options,
            }
        })

    def _get_value(self, serviceName, path, defaultValue=None):
        return self.monitor.get_value(serviceName, path, defaultValue)

    def _safeadd(self, *args):
        """Add values, treating None as 0. Returns None if all args are None."""
        result = None
        for arg in args:
            if arg is not None:
                if result is None:
                    result = arg
                else:
                    result += arg
        return result

    def _get_battery(self):
        """Get battery power and voltage from battery monitor or vebus."""
        # First try battery monitor
        batteries = self.monitor.get_service_list('com.victronenergy.battery')
        if batteries:
            service = next(iter(batteries.keys()))
            p = self._get_value(service, '/Dc/0/Power')
            v = self._get_value(service, '/Dc/0/Voltage')
            if p is not None and v is not None and v > 0:
                return float(p), float(v)
        
        # Fall back to vebus
        vebusses = self.monitor.get_service_list('com.victronenergy.vebus')
        if vebusses:
            service = next(iter(vebusses.keys()))
            v = self._get_value(service, '/Dc/0/Voltage')
            i = self._get_value(service, '/Dc/0/Current')
            if v is not None and i is not None and v > 0:
                return float(v * i), float(v)
        
        return None, None

    def _get_solar_power(self):
        """Sum of all solar charger power (including load output)."""
        total = 0.0
        for s in self.monitor.get_service_list('com.victronenergy.solarcharger') or {}:
            v = self._get_value(s, '/Dc/0/Voltage')
            i = self._get_value(s, '/Dc/0/Current')
            l = self._get_value(s, '/Load/I', 0) or 0
            if v is not None and i is not None:
                total += float(v) * (float(i) + float(l))
        return total

    def _get_charger_power(self):
        """Sum of all AC charger power."""
        total = 0.0
        for s in self.monitor.get_service_list('com.victronenergy.charger') or {}:
            v = self._get_value(s, '/Dc/0/Voltage')
            i = self._get_value(s, '/Dc/0/Current')
            if v is not None and i is not None:
                total += float(v) * float(i)
        return total

    def _get_fuelcell_power(self):
        """Sum of all fuel cell power."""
        total = 0.0
        for s in self.monitor.get_service_list('com.victronenergy.fuelcell') or {}:
            v = self._get_value(s, '/Dc/0/Voltage')
            i = self._get_value(s, '/Dc/0/Current')
            if v is not None and i is not None:
                total += float(v) * float(i)
        return total

    def _get_alternator_power(self):
        """Sum of all alternator power."""
        total = 0.0
        for s in self.monitor.get_service_list('com.victronenergy.alternator') or {}:
            p = self._get_value(s, '/Dc/0/Power')
            if p is not None:
                total += float(p)
        return total

    def _get_vebus_dc_power(self):
        """Sum of all VE.Bus DC power (negative when inverting, positive when charging)."""
        total = 0.0
        for s in self.monitor.get_service_list('com.victronenergy.vebus') or {}:
            v = self._get_value(s, '/Dc/0/Voltage')
            i = self._get_value(s, '/Dc/0/Current')
            if v is not None and i is not None:
                total += float(v) * float(i)
        return total

    def _get_dcsource_power(self):
        """Sum of all DC source power (e.g., wind generators)."""
        total = 0.0
        for s in self.monitor.get_service_list('com.victronenergy.dcsource') or {}:
            p = self._get_value(s, '/Dc/0/Power')
            if p is None:
                v = self._get_value(s, '/Dc/0/Voltage')
                i = self._get_value(s, '/Dc/0/Current')
                if v is not None and i is not None:
                    p = float(v) * float(i)
                else:
                    p = 0
            total += float(p)
        return total

    def _get_dcload_power(self):
        """Sum of all DC load power."""
        total = 0.0
        for s in self.monitor.get_service_list('com.victronenergy.dcload') or {}:
            p = self._get_value(s, '/Dc/0/Power')
            if p is None:
                v = self._get_value(s, '/Dc/0/Voltage')
                i = self._get_value(s, '/Dc/0/Current')
                if v is not None and i is not None:
                    p = float(v) * float(i)
                else:
                    p = 0
            total += float(p)
        return total

    def _get_ac_consumption(self):
        """DEPRECATED: VE.Bus DC power already accounts for AC consumption."""
        return 0.0

    def update(self):
        """Calculate DC system: Sources + VEBus_DC - Battery - Known DC Loads = Unknown DC System."""
        # Get battery values
        battery_power, battery_voltage = self._get_battery()
        
        if battery_power is None or battery_voltage is None:
            # No valid battery data, fallback to simple aggregation
            return self._update_simple_aggregation()

        # Sum all DC sources
        solar = self._get_solar_power()
        chargers = self._get_charger_power()
        fuel = self._get_fuelcell_power()
        alternator = self._get_alternator_power()
        wind = self._get_dcsource_power()
        vebus_dc = self._get_vebus_dc_power()  # Can be negative when inverting
        
        # Sum known DC loads
        dcloads = self._get_dcload_power()

        # Calculate unknown DC system consumption
        # DC_system = All_Sources + VEBus_DC - Battery - Known_DC_Loads
        # (VEBus_DC is negative when inverting, so it reduces the total sources)
        dc_system = (solar + chargers + fuel + alternator + wind + vebus_dc) - battery_power - dcloads

        # Update values
        self._local_values["/Dc/0/Voltage"] = battery_voltage
        self._local_values["/Dc/0/Power"] = dc_system
        self._local_values["/Dc/0/Current"] = dc_system / battery_voltage if battery_voltage > 0 else 0

        # logger.info(
        #     "DC System → solar=%.0fW chargers=%.0fW fuel=%.0fW alt=%.0fW wind=%.0fW vebus_dc=%.0fW dcloads=%.0fW battery=%.0fW ⇒ system=%.0fW",
        #     solar, chargers, fuel, alternator, wind, vebus_dc, dcloads, battery_power, dc_system
        # )
        
        return True

    def _update_simple_aggregation(self):
        """Fallback: simple aggregation of dcload and dcsource when no battery available."""
        # Collect all dcload and dcsource services
        dcloads = self.monitor.get_service_list('com.victronenergy.dcload')
        dcsources = self.monitor.get_service_list('com.victronenergy.dcsource')

        # Initialize aggregated values
        totalCurrent = None
        totalPower = None
        voltageSum = 0
        voltageCount = 0
        totalEnergyIn = 0
        totalEnergyOut = 0
        maxLowVoltageAlarm = ALARM_OK
        maxHighVoltageAlarm = ALARM_OK
        maxLowTempAlarm = ALARM_OK
        maxHighTempAlarm = ALARM_OK

        # Process DC loads (positive = consuming power)
        for serviceName in dcloads:
            voltage = self._get_value(serviceName, "/Dc/0/Voltage")
            current = self._get_value(serviceName, "/Dc/0/Current")
            power = self._get_value(serviceName, "/Dc/0/Power")
            
            # Calculate power if not provided
            if power is None and voltage is not None and current is not None:
                power = voltage * current
            
            # Aggregate current and power
            totalCurrent = self._safeadd(totalCurrent, current)
            totalPower = self._safeadd(totalPower, power)
            
            # Aggregate voltage (only non-zero voltages)
            if voltage is not None and voltage > VOLTAGE_DEADBAND:
                voltageSum += voltage
                voltageCount += 1
            
            # Aggregate energy
            energyIn = self._get_value(serviceName, "/History/EnergyIn", 0)
            if energyIn is not None:
                totalEnergyIn += energyIn
            
            # Aggregate alarms
            maxLowVoltageAlarm = max(
                self._get_value(serviceName, "/Alarms/LowVoltage", ALARM_OK), 
                maxLowVoltageAlarm)
            maxHighVoltageAlarm = max(
                self._get_value(serviceName, "/Alarms/HighVoltage", ALARM_OK), 
                maxHighVoltageAlarm)
            maxLowTempAlarm = max(
                self._get_value(serviceName, "/Alarms/LowTemperature", ALARM_OK), 
                maxLowTempAlarm)
            maxHighTempAlarm = max(
                self._get_value(serviceName, "/Alarms/HighTemperature", ALARM_OK), 
                maxHighTempAlarm)

        # Process DC sources (negate current/power to make it negative = generating)
        for serviceName in dcsources:
            voltage = self._get_value(serviceName, "/Dc/0/Voltage")
            current = self._get_value(serviceName, "/Dc/0/Current")
            power = self._get_value(serviceName, "/Dc/0/Power")
            
            # Calculate power if not provided
            if power is None and voltage is not None and current is not None:
                power = voltage * current
            
            # Negate for sources (they provide power, so negative in system context)
            if current is not None:
                current = -current
            if power is not None:
                power = -power
            
            # Aggregate current and power
            totalCurrent = self._safeadd(totalCurrent, current)
            totalPower = self._safeadd(totalPower, power)
            
            # Aggregate voltage (only non-zero voltages)
            if voltage is not None and voltage > VOLTAGE_DEADBAND:
                voltageSum += voltage
                voltageCount += 1
            
            # Aggregate energy
            energyOut = self._get_value(serviceName, "/History/EnergyOut", 0)
            if energyOut is not None:
                totalEnergyOut += energyOut
            
            # Aggregate alarms
            maxLowVoltageAlarm = max(
                self._get_value(serviceName, "/Alarms/LowVoltage", ALARM_OK), 
                maxLowVoltageAlarm)
            maxHighVoltageAlarm = max(
                self._get_value(serviceName, "/Alarms/HighVoltage", ALARM_OK), 
                maxHighVoltageAlarm)
            maxLowTempAlarm = max(
                self._get_value(serviceName, "/Alarms/LowTemperature", ALARM_OK), 
                maxLowTempAlarm)
            maxHighTempAlarm = max(
                self._get_value(serviceName, "/Alarms/HighTemperature", ALARM_OK), 
                maxHighTempAlarm)

        # Update local values
        self._local_values["/Dc/0/Voltage"] = voltageSum / voltageCount if voltageCount > 0 else None
        self._local_values["/Dc/0/Current"] = totalCurrent if totalCurrent is not None else 0
        self._local_values["/Dc/0/Power"] = totalPower if totalPower is not None else 0
        self._local_values["/History/EnergyIn"] = totalEnergyIn
        self._local_values["/History/EnergyOut"] = totalEnergyOut
        self._local_values["/Alarms/LowVoltage"] = maxLowVoltageAlarm
        self._local_values["/Alarms/HighVoltage"] = maxHighVoltageAlarm
        self._local_values["/Alarms/LowTemperature"] = maxLowTempAlarm
        self._local_values["/Alarms/HighTemperature"] = maxHighTempAlarm
        
        return True

    def publish(self):
        for k,v in self._local_values.items():
            self.service[k] = v
        return True

    def __str__(self):
        return PRODUCT_NAME


def main():
    DBusGMainLoop(set_as_default=True)
    dcSystem = DCSystemService(dbusConnection())
    GLib.timeout_add(200, dcSystem.update)
    GLib.timeout_add_seconds(1, dcSystem.publish)
    logger.info("Registered DC System Aggregator")
    mainloop = GLib.MainLoop()
    mainloop.run()


if __name__ == "__main__":
    main()

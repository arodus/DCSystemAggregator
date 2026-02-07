# DC System Aggregator

A DC system DBus service that aggregates multiple DC loads and sources for Victron Energy GX devices.

## Overview

This service calculates unmeasured DC system consumption using the same formula used by victron:

**DC System = (Solar + Chargers + Fuel + Alternator + Wind + VE.Bus_DC) - Battery - Known_DC_Loads**

If you have `dcload` and `dcsource` DBus services but no DC system measurement, this aggregator provides the missing data for the VRM portal and GX device UI to display total DC system power correctly.

## Forked From

This project is forked from [pulquero/DCSystemAggregator:main](https://github.com/pulquero/DCSystemAggregator), with enhancements to support:
- Full Victron-style DC system calculation
- Integration with `com.victronenergy.dcload` and `com.victronenergy.dcsource` services
- Proper handling of VE.Bus inverter/charger DC power
- Voltage averaging across multiple DC devices

## Installation

**This package must be manually added to [SetupHelper](https://github.com/kwindrem/SetupHelper)** as it is not included in the default package list.


# RTIS Bulk Analysis Dashboard

A modular web application for analyzing Railway Signal (SNT) events against RTIS GPS data and FSD master logs to detect signal violations.

## Files
- `index.html`: Main dashboard structure (split from monolithic v21.1)
- `style.css`: Application styling (v21.1)
- `app.js`: Core analysis logic and map/chart rendering (v21.1)

## Setup
Simply open `index.html` in a modern web browser. All dependencies are loaded via CDN.

## Features
- RTIS vs SNT time-window analysis
- Automatic/Fuzzy station mapping
- Speed-time graph analysis
- Individual PDF violation reports
- KML export for Google Earth

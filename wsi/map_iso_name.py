# -*- coding: utf-8 -*-
"""standard ISO to CEVAW standard name mapping"""

# establishes which countries are in Asia Pacific and their standardised name
ALL_ISO_NAME = {
    "AFG": "Afghanistan",
    "AUS": "Australia",
    "BGD": "Bangladesh",
    "BTN": "Bhutan",
    "BRN": "Brunei",
    "KHM": "Cambodia",
    "KOR": "Republic of Korea (South Korea)",
    "PRK": "Democratic People's Republic of Korea (North Korea)",
    "IND": "India",
    "IDN": "Indonesia",
    "JPN": "Japan",
    "LAO": "Laos",
    "MYS": "Malaysia",
    "MDV": "Maldives",
    "MNG": "Mongolia",
    "MMR": "Myanmar",
    "NPL": "Nepal",
    "NZL": "New Zealand",
    "PAK": "Pakistan",
    "CHN": "China",
    "PHL": "Philippines",
    "SGP": "Singapore",
    "LKA": "Sri Lanka",
    "TWN": "Taiwan",
    "THA": "Thailand",
    "TLS": "Timor-Leste",  # economy code is 'TMP'
    "VNM": "Vietnam",
    "NCL": "New Caledonia",
    "COK": "Cook Islands",
    "FSM": "Micronesia",
    "PNG": "Papua New Guinea",
    "WSM": "Samoa",
    "TON": "Tonga",
    "NIU": "Niue",
    "FJI": "Fiji",
    "KIR": "Kiribati",
    "MHL": "Marshall Islands",
    "NRU": "Nauru",
    "PLW": "Palau",
    "VUT": "Vanuatu",
    "SLB": "Solomon Islands",
    "TUV": "Tuvalu",
    "HKG": "Hong Kong",
    "TKL": "Tokelau",
    "WLF": "Wallis and Futuna Islands",
    "MNP": "Northern Mariana Islands",
    "ASM": "American Samoa",
    "PYF": "French Polynesia",
}

EXCLUDE_ISO = {
    "TKL",
    "WLF",
    "MNP",
    "ASM",
    "PYF",
}

# Active ISO_NAME map by filtering out the excludes
ISO_NAME = {iso: name for iso, name in ALL_ISO_NAME.items() if iso not in EXCLUDE_ISO}

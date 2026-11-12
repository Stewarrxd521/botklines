import os

# Las pruebas no consultan al executor real: las que prueban los precios del executor lo activan a mano.
os.environ.setdefault("EXECUTOR_FILLS", "false")
os.environ.setdefault("BOTKLINE_AUTOSTART", "false")

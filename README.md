Quick & dirty MQTT-based telemetry archiver & viewer.

Example config, `~/.ctelemetryrc`:

```cl
(setf *mqtt-host* "localhost"
      *mqtt-port* 1883
      *db-file* "/var/lib/ctelemetry/telemetry.db"
      *www-port* 8999)

(add-subscription-topic "/whatever/devices/wb-w1/controls/00042d40ffff")
(add-subscription-topic "/whatever/devices/wb-w1/controls/0000058e1692")
(add-subscription-topic "/+/events/#")

(define-topic "/whatever/devices/wb-w1/controls/0000058e1692" "Outside"
  '(("0000058e1692" "Temperature")))

(define-topic "/whatever/devices/wb-w1/controls/00042d40ffff" "Home"
  '(("00042d40ffff" "Temperature")))

(define-section "/fionbio/" "Sensors at home")
```

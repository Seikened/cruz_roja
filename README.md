*ROADMAP TÉCNICO & ARQUITECTURA DEL ECOSISTEMA - PROYECTO CRUZ ROJA* 🚑

Equipo, les comparto el desglose técnico detallado de los 4 pilares operativos. El objetivo no es solo digitalizar, sino crear una arquitectura unificada donde el Chatbot es la interfaz y la infraestructura el cerebro que orquesta todo.

Aquí el detalle de los alcances:

---

*1️⃣ PILAR 1: DIGITALIZACIÓN & INGESTA (El "Levantamiento")*
No es solo escanear; es trasladar la lógica del papel a una estructura de datos explotable.
🔸 *Mapeo de Formatos:* Análisis profundo de los expedientes físicos actuales para definir el esquema digital (JSON Schema) y discriminar datos críticos vs. opcionales.
🔸 *Motor de Procesamiento:* Desarrollo de scripts en Python que reciban la imagen, apliquen filtros (limpieza/escala de grises) y ejecuten el OCR para extracción de texto.
🔸 *Validación de Entrada:* Reglas de negocio para asegurar que lo que entra al sistema sea coherente antes de tocar la base de datos.

*2️⃣ PILAR 2: INFRAESTRUCTURA, DATOS & ANALYTICS (El Núcleo)*
Este es el pilar más denso. Es la "columna vertebral" que conecta la digitalización con el Chatbot.
🔸 *Migración de Legacy (Excel):* Tratamiento y limpieza de los datos históricos actuales (ETL). El objetivo es dejar de usar Excel como base de datos y migrar a un motor robusto (PostgreSQL/Mongo).
🔸 *Arquitectura de Servidor:* Configuración del entorno (VPS + Docker) que sostendrá los scripts de automatización y la API.
🔸 *Dashboarding:* Al tener datos limpios, generamos tableros de inteligencia para visualizar KPIs de pacientes, zonas y patologías en tiempo real.
🔸 *Orquestación:* Crear la lógica que permite que el Chatbot "hable" con la Base de Datos y viceversa.

*3️⃣ PILAR 3: CHATBOT INTERACTIVO (La Interfaz)*
El bot funge como el puente operativo para usuarios internos y externos, reduciendo la fricción tecnológica.
🔸 *Asistente de Ingesta:* Flujos automatizados para que el personal cargue expedientes mediante fotos directamente en WhatsApp.
🔸 *Consultas en Tiempo Real:* Capacidad de consultar estatus o información de la BD sin necesidad de acceder a una computadora.
🔸 *Ecosistema:* Integración total con la infraestructura del Pilar 2; el bot no es estático, es una terminal operativa conectada al servidor.

*4️⃣ PILAR 4: IMÁGENES MÉDICAS (Rxs)* ⚠️ _(Estado: Standby)_
🔸 *Investigación:* Se mantiene como un módulo opcional. Actualmente, el software propietario de los equipos (FCR PRIMA) protege los archivos `.dcm`, lo que impide una extracción directa sin comprometer garantías o sistemas.
🔸 *Estrategia:* No bloquea el desarrollo de los otros 3 pilares. Se abordará si se logra acceso técnico seguro.

---

*📌 CONCLUSIÓN TÉCNICA:*
Estamos construyendo un ecosistema donde la *Infraestructura (P2)* soporta la carga de trabajo pesada, mientras que la *Digitalización (P1)* alimenta el sistema y el *Chatbot (P3)* facilita el acceso. Todo centralizado para evitar la fragmentación de información.





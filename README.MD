# Spotify → TIDAL Playlist Migrator (Poetry + Python)

Migra tus playlists de **Spotify** a **TIDAL** de forma automatizada.  
Lee tus playlists con la **Spotify Web API** y crea/equipa las listas en TIDAL usando llamadas HTTP autenticadas (token de `tidalapi`).  
Optimiza el _matching_ por **ISRC** y, si no hay, usa búsqueda por texto con filtro de duración (±2s).

> **Nota:** Este proyecto usa `tidalapi` solo para autenticación; las llamadas a la API de TIDAL se hacen con `requests` (controlamos ETag y headers). No es un proyecto oficial de TIDAL.

---

## ✨ Características

- 🧭 Migra **todas** tus playlists (o filtra por nombre parcial)
- 🎯 _Matching_ por **ISRC** (máxima precisión)
- 🔎 _Fallback_ por `título + artista` con **tolerancia por duración**
- 🧱 Inserción en **lotes** (configurable) con manejo de ETag (If-None-Match)
- 🧾 Genera `not_found.csv` con los temas que no se pudieron mapear

---

## ⚙️ Requisitos

- **Python** 3.10–3.12
- **Poetry** (gestor de dependencias)
- Cuenta de **Spotify** y **TIDAL**
- Acceso al **Spotify Developer Dashboard** para crear tu app y obtener `client_id`/`client_secret`

---

## 🚀 Instalación rápida

```bash
# 1) Clona el repo
git clone https://github.com/<tu-org>/<tu-repo>.git
cd <tu-repo>

# 2) Instala Poetry (si no lo tienes)
# macOS/Linux:
#   pipx install poetry
# Windows (PowerShell):
#   (Invoke-WebRequest -Uri https://install.python-poetry.org -UseBasicParsing).Content | py -

# 3) Instala dependencias y crea el venv
poetry install
```

## 🔐 Configura credenciales

### 1. Crea una app en Spotify

1. Ve a Spotify Developer Dashboard → Create app y asegurate de copiar el client id y client secret y establecer el url de callback.
2. En Redirect URIs, agrega exactamente: http://127.0.0.1:8080/callback
3. Copia tu Client ID y Client Secret.

### 2) Crea el archivo .env (recuerda no subirlo a tu repo porque tiene información sensible)

```env
# Spotify OAuth
SPOTIFY_CLIENT_ID=tu_client_id
SPOTIFY_CLIENT_SECRET=tu_client_secret
SPOTIFY_REDIRECT_URI=http://127.0.0.1:8080/callback

# (Opcional) Migra solo playlists cuyo nombre contenga esta cadena (case-insensitive)
PLAYLIST_NAME_FILTER=

# País de catálogo TIDAL para búsquedas (afecta resultados)
TIDAL_COUNTRY=PE

# Tamaño del lote al insertar pistas en TIDAL
TIDAL_ADD_BATCH=100
```

Nota: SPOTIFY_REDIRECT_URI debe coicidir con el callback establecido en tu app de spotify

## 🧩 Estructura del proyecto

```bash
.
├─ pyproject.toml
├─ .env
└─ src/
   └─ spotify_to_tidal/
      └─ main.py
```

## ▶️ Ejecución

# Opción 1: directo

```bash
poetry run spotify-to-tidal
```

Flujo esperado:

Se abre el navegador para autorizar Spotify (primer uso).

En consola verás un enlace de TIDAL para autorizar (código expira en 300 s).

El script creará cada playlist en TIDAL y añadirá las pistas en lotes.

Si hay temas no encontrados, generará not_found.csv.

## 🛠️ Cómo funciona (resumen técnico)

Spotify: se listan playlists y pistas (con external_ids.isrc cuando existe).

Resolución TIDAL:

Primero se busca por ISRC (GET /v1/search?query=isrc:<ISRC>&type=tracks).

Si no hay ISRC, se busca "<title> <artist>", y se filtra por duración ±2s.

Creación de playlists en TIDAL:

POST /v1/users/{userId}/playlists.

Inserción en lotes:

POST /v1/playlists/{uuid}/items requiere ETag vigente vía If-None-Match.

Antes de cada lote se obtiene el ETag actual con GET /v1/playlists/{uuid}/items?limit=1.

Si hay 412 (mismatch de ETag), se reintenta 1 vez con el ETag nuevo.

## ⚙️ Parámetros útiles

Filtrar playlists por nombre: edita PLAYLIST_NAME_FILTER en .env (ej. rock, workout).

Tamaño de lote: TIDAL_ADD_BATCH (baja a 50 si ves rate-limits).

Catálogo TIDAL: cambia TIDAL_COUNTRY (ej. US) si hay muchos “no encontrados”.

## 🧪 Consejos de calidad de mapeo

ISRC primero: evita versiones live/remaster.

Fallback por texto + duración: útil cuando falta ISRC.

Disponibilidad regional: prueba otro TIDAL_COUNTRY.

Revisa not_found.csv: suelen ser pocos, pero útiles para ajustes manuales.

## ❗ Problemas comunes

INVALID_CLIENT: Invalid redirect URI (Spotify)
Asegúrate que el Redirect URI en Dashboard coincide exactamente con SPOTIFY_REDIRECT_URI.

412 You must send the correct Etag… (TIDAL)
Ya está implementado el manejo de ETag. Si lo ves, fue un choque puntual y el script reintenta.

401/403 (TIDAL)
Token expirado: repite el login.

Demasiados “no encontrados”
Cambia TIDAL_COUNTRY a US y reintenta.

Rate limit
Baja TIDAL_ADD_BATCH (p. ej. 50) y/o aumenta sleep_s.

## 🧹 Mantenimiento

Borrar cache de Spotify:

```bash
rm -f .spotify_cache
```

## 🧑‍💻 Contribuir

¡PRs bienvenidos! Ideas útiles:

- DRY_RUN=1 para simular y solo generar not_found.csv
- Reutilizar playlist si ya existe (por título) en lugar de crear una nueva
- Soporte para exclusiones/inclusiones por patrón
- Tests sobre la lógica de resolución (ISRC / fuzzy match)

## ⚖️ Licencia

MIT. Consulta el archivo LICENSE.

import os
import time
import math
import csv
from dotenv import load_dotenv
import pandas as pd
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import tidalapi
import requests

load_dotenv()

SPOTIFY_CLIENT_ID     = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
SPOTIFY_REDIRECT_URI  = os.getenv("SPOTIFY_REDIRECT_URI", "http://localhost:8080/callback")
PLAYLIST_NAME_FILTER  = (os.getenv("PLAYLIST_NAME_FILTER") or "").strip().lower()
TIDAL_COUNTRY         = os.getenv("TIDAL_COUNTRY", "PE")
TIDAL_ADD_BATCH       = int(os.getenv("TIDAL_ADD_BATCH", "100"))

TIDAL_API_BASE = "https://api.tidal.com/v1"

def _tidal_headers(session):
    token_type = getattr(session, "token_type", "Bearer")
    access_token = getattr(session, "access_token", None)
    if not access_token:
        raise RuntimeError("No access_token en la sesión TIDAL. ¿Falló el login?")
    return {
        "Authorization": f"{token_type} {access_token}",
        "User-Agent": "spotify-to-tidal/0.1",
    }

def tidal_http(session, method, path, params=None, data=None):
    url = f"{TIDAL_API_BASE}/{path.lstrip('/')}"
    headers = _tidal_headers(session)
    resp = requests.request(method, url, headers=headers, params=params, data=data, timeout=30)
    if resp.status_code >= 400:
        raise RuntimeError(f"TIDAL API {method} {url} -> {resp.status_code}: {resp.text}")
    # algunas respuestas devuelven JSON, otras 204 No Content
    return resp.json() if resp.content else {}

def tidal_http_raw(session, method, path, params=None, data=None, headers_extra=None):
    url = f"{TIDAL_API_BASE}/{path.lstrip('/')}"
    headers = _tidal_headers(session)
    if headers_extra:
        headers.update(headers_extra)
    resp = requests.request(method, url, headers=headers, params=params, data=data, timeout=30)
    return resp  # devolvemos el Response tal cual

def tidal_get_playlist_etag(session, playlist_id):
    """
    Lee el ETag de la playlist llamando a:
    GET /v1/playlists/{uuid}/items?limit=1
    (No importa el contenido, solo queremos el header 'ETag')
    """
    path = f"playlists/{playlist_id}/items"
    params = {"countryCode": TIDAL_COUNTRY or session.country_code, "limit": 1}
    resp = tidal_http_raw(session, "GET", path, params=params)
    if resp.status_code >= 400:
        raise RuntimeError(f"TIDAL API GET ETag {resp.url} -> {resp.status_code}: {resp.text}")
    etag = resp.headers.get("ETag")
    if not etag:
        # Algunos proxies podrían devolverlo como ETag en minúsculas, pero requests normaliza.
        raise RuntimeError("No se pudo obtener el ETag de la playlist (header 'ETag' ausente).")
    return etag


############################################################
# Spotify: auth + helpers
############################################################
def spotify_client():
    scope = "playlist-read-private playlist-read-collaborative"
    auth = SpotifyOAuth(
        client_id=SPOTIFY_CLIENT_ID,
        client_secret=SPOTIFY_CLIENT_SECRET,
        redirect_uri=SPOTIFY_REDIRECT_URI,
        scope=scope,
        open_browser=True,
        cache_path=".spotify_cache"
    )
    return spotipy.Spotify(auth_manager=auth)

def sp_paginate(sp, method, *args, limit=50, **kwargs):
    """Paginar resultados para métodos de spotipy que aceptan limit/offset."""
    offset = 0
    items = []
    while True:
        page = method(*args, limit=limit, offset=offset, **kwargs)
        data = page.get('items') or page.get('tracks', {}).get('items')
        if data is None:
            data = page['items']
        items.extend(data)
        if page.get('next'):
            offset += limit
        else:
            break
    return items

def sp_current_user_playlists(sp):
    return sp_paginate(sp, sp.current_user_playlists, limit=50)

def sp_playlist_tracks(sp, playlist_id):
    tracks = []
    offset = 0
    limit = 100
    while True:
        resp = sp.playlist_items(playlist_id, offset=offset, limit=limit, additional_types=['track'])
        items = resp.get('items', [])
        for it in items:
            tr = it.get('track')
            if not tr:
                continue
            name = tr.get('name')
            album = (tr.get('album') or {}).get('name')
            artists = ", ".join(a.get('name') for a in (tr.get('artists') or []))
            duration_ms = tr.get('duration_ms') or 0
            isrc = None
            ext = tr.get('external_ids') or {}
            if 'isrc' in ext:
                isrc = ext['isrc']
            tracks.append({
                "name": name,
                "album": album,
                "artists": artists,
                "duration_ms": duration_ms,
                "isrc": isrc
            })
        if resp.get('next'):
            offset += limit
        else:
            break
    return tracks

############################################################
# TIDAL: auth + helpers (via tidalapi)
############################################################
def tidal_client():
    session = tidalapi.Session()
    print("== Inicia sesión en tu cuenta TIDAL ==")
    # Login OAuth simplificado que abre enlace para autorizar tu cuenta
    session.login_oauth_simple()
    print(f"TIDAL login OK. Country: {session.country_code}")
    return session

def tidal_create_playlist(session, name, description="Migrated from Spotify", public=True):
    """
    Crea playlist vía API HTTP:
    POST /v1/users/{userId}/playlists
    """
    path = f"users/{session.user.id}/playlists"
    params = {"countryCode": TIDAL_COUNTRY or session.country_code}
    data = {
        "title": name[:255],
        "description": (description or "")[:1000],
        "public": "true" if public else "false",
    }
    resp = tidal_http(session, "POST", path, params=params, data=data)
    playlist_id = resp.get("uuid") or resp.get("id")
    if not playlist_id:
        raise RuntimeError(f"No se obtuvo ID/UUID de la playlist. Respuesta: {resp}")

    class _P:  # objeto mínimo con .id
        pass
    p = _P()
    p.id = playlist_id
    return p



def tidal_search_track_by_isrc(session, isrc):
    """
    GET /v1/search?query=isrc:XXXX&type=tracks
    """
    if not isrc:
        return None
    params = {
        "query": f"isrc:{isrc}",
        "type": "tracks",
        "limit": 5,
        "countryCode": TIDAL_COUNTRY or session.country_code
    }
    resp = tidal_http(session, "GET", "search", params=params)
    tracks = ((resp or {}).get("tracks") or {}).get("items") or []
    return tracks[0] if tracks else None


def tidal_search_track_fuzzy(session, title, artists, duration_ms):
    """
    GET /v1/search?query=<title+artists>&type=tracks
    y filtramos por duración ±2s
    """
    q = f"{title} {artists}".strip()
    if not q:
        return None
    params = {
        "query": q,
        "type": "tracks",
        "limit": 20,
        "countryCode": TIDAL_COUNTRY or session.country_code
    }
    resp = tidal_http(session, "GET", "search", params=params)
    tracks = ((resp or {}).get("tracks") or {}).get("items") or []
    if not tracks:
        return None
    target = duration_ms or 0
    for t in tracks:
        dur_ms = int(t.get("duration", 0)) * 1000  # TIDAL devuelve segundos
        if abs(dur_ms - target) <= 2000:
            return t
    return tracks[0]


def tidal_add_tracks_in_batches(session, playlist, track_ids, batch=100, sleep_s=0.3):
    """
    POST /v1/playlists/{uuid}/items requiere 'If-None-Match' con el ETag actual.
    Como el ETag cambia tras cada modificación, lo refrescamos ANTES de cada POST.
    """
    for i in range(0, len(track_ids), batch):
        chunk = track_ids[i:i+batch]
        # 1) Obtener ETag actual
        etag = tidal_get_playlist_etag(session, playlist.id)

        # 2) POST con If-None-Match: <etag>
        path = f"playlists/{playlist.id}/items"
        params = {"countryCode": TIDAL_COUNTRY or session.country_code}
        data = {"trackIds": ",".join(str(t) for t in chunk)}
        headers_extra = {"If-None-Match": etag}

        resp = tidal_http_raw(session, "POST", path, params=params, data=data, headers_extra=headers_extra)

        if resp.status_code == 412:
            # ETag desactualizado (carrera). Reintentar una sola vez con ETag nuevo.
            time.sleep(0.5)
            etag = tidal_get_playlist_etag(session, playlist.id)
            headers_extra = {"If-None-Match": etag}
            resp = tidal_http_raw(session, "POST", path, params=params, data=data, headers_extra=headers_extra)

        if resp.status_code >= 400:
            raise RuntimeError(f"TIDAL API POST {resp.url} -> {resp.status_code}: {resp.text}")

        # Pausa para no toparse con rate limits
        time.sleep(sleep_s)



############################################################
# Migración
############################################################
def migrate_all_playlists(playlist_filter=""):
    sp = spotify_client()
    session = tidal_client()

    sp_pls = sp_current_user_playlists(sp)
    if playlist_filter:
        sp_pls = [pl for pl in sp_pls if playlist_filter in (pl.get('name','').lower())]

    print(f"Playlists Spotify a migrar: {len(sp_pls)}")
    not_found_rows = []

    for idx, pl in enumerate(sp_pls, 1):
        name = pl.get('name')
        description = (pl.get('description') or "") + " (imported from Spotify)"
        print(f"\n[{idx}/{len(sp_pls)}] Migrando playlist: {name}")

        # 1) Leer tracks de Spotify
        tracks = sp_playlist_tracks(sp, pl['id'])
        print(f"  - {len(tracks)} temas a resolver")

        # 2) Crear playlist en TIDAL
        t_playlist = tidal_create_playlist(session, name, description)
        print(f"  - TIDAL playlist creada: {t_playlist.id}")

        # 3) Resolver tracks -> TIDAL IDs
        tidal_ids = []
        for t in tracks:
            tid = None
            # a) Intento por ISRC
            if t['isrc']:
                hit = tidal_search_track_by_isrc(session, t['isrc'])
                if hit:
                    tid = hit.get('id')
            # b) Fallback fuzzy
            if not tid:
                hit = tidal_search_track_fuzzy(session, t['name'], t['artists'], t['duration_ms'])
                if hit:
                    tid = hit.get('id')

            if tid:
                tidal_ids.append(tid)
            else:
                not_found_rows.append({
                    "playlist": name,
                    "title": t['name'],
                    "artists": t['artists'],
                    "album": t['album'],
                    "isrc": t['isrc'] or "",
                    "duration_ms": t['duration_ms']
                })

        # 4) Agregar en lotes
        if tidal_ids:
            tidal_add_tracks_in_batches(session, t_playlist, tidal_ids, batch=TIDAL_ADD_BATCH)
        print(f"  - Agregados {len(tidal_ids)}/{len(tracks)}")

    # 5) Reporte de no encontrados
    if not_found_rows:
        out = "not_found.csv"
        pd.DataFrame(not_found_rows).to_csv(out, index=False, quoting=csv.QUOTE_MINIMAL)
        print(f"\n==> Reporte generado: {out} (pistas no encontradas o ambiguas)")
    else:
        print("\n==> ¡Todo encontrado!")

def main():
    migrate_all_playlists(os.getenv("PLAYLIST_NAME_FILTER", "").strip().lower())

if __name__ == "__main__":
    main()

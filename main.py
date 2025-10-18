import requests
from typing import Optional

# Adres URL do pobrania, zgodnie z treścią zadania
PLIK_URL = "https://oleksandr-fedoruk.com/wp-content/uploads/2025/10/sample.csv"
PLIK_URL_404 = "https://httpstat.us/404"  # Dodatkowy URL do testowania 404
PLIK_URL_503 = "https://httpstat.us/503"  # Dodatkowy URL do testowania 503

# =================================================================
# NOWE KLASY WYJĄTKÓW (zgodnie z zadaniem)
# =================================================================

class DownloadError(Exception):
    """Bazowa klasa dla wyjątków związanych z pobieraniem plików."""
    pass

class NotFoundError(DownloadError):
    """Wyjątek rzucany, gdy plik nie zostanie znaleziony (kod 404)."""
    pass

class AccessDeniedError(DownloadError):
    """Wyjątek rzucany, gdy dostęp do pliku jest zabroniony (kod 503)."""
    pass

# =================================================================


def download_file(url: str, filename: Optional[str] = "latest.csv") -> bool:
    if not url:
        raise ValueError("Adres URL nie może być pusty.")

    if not filename.lower().endswith(".csv"):
        final_filename = filename + ".csv"
    else:
        final_filename = filename

    print(f"\nPobieranie pliku z: {url}")
    print(f"Zapisywanie jako: {final_filename}")

    try:
        response = requests.get(url, stream=True)
        response.raise_for_status() 

        with open(final_filename, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    file.write(chunk)

        print(f"\nPlik został zapisany jako '{final_filename}'.")
        return True

    except requests.exceptions.HTTPError as e:
        status_code = e.response.status_code       
        if status_code == 404:
            raise NotFoundError(f"Plik pod adresem '{url}' nie został znaleziony (HTTP 404).")
        elif status_code == 503:
            raise AccessDeniedError(f"Serwer tymczasowo niedostępny lub usługa zabroniona (HTTP 503).")
        else:
            raise DownloadError(f"Wystąpił błąd HTTP: {status_code} ({e})")
            
    except requests.exceptions.RequestException as e:
        raise DownloadError(f"Błąd połączenia: {e}")
    except IOError as e:
        raise DownloadError(f"Błąd podczas zapisu pliku na dysku: {e}")

if __name__ == "__main__":
    print("--- Program do pobierania plików CSV z niestandardową obsługą błędów ---")
    user_filename = input("Podaj nazwę pliku do zapisu (puste pole to 'latest.csv'): ")
    cleaned_filename = user_filename.strip()
    urls_to_test = [
        (PLIK_URL, cleaned_filename if cleaned_filename else "test_sukces"),
        (PLIK_URL_404, "test_404"),
        (PLIK_URL_503, "test_503")
    ]
    
    for url, name in urls_to_test:
        try:
            print("\n" + "="*50)
            download_file(url, name)
        except NotFoundError as e:
            print(f"\nBŁĄD: Plik nie istnieje! ({e})")
        except AccessDeniedError as e:
            print(f"\nBŁĄD: Dostęp zabroniony! ({e})")
        except DownloadError as e:
            print(f"\nBŁĄD: Błędne pobieranie danych! ({e})")
        except Exception as e:
            print(f"\nNieznany błąd! ({e})")

    print("\n--- Zakończono działanie programu ---")
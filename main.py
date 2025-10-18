import requests
import csv
import time
from functools import wraps
from typing import Optional, Generator, Tuple, List, Any

PLIK_URL = "https://oleksandr-fedoruk.com/wp-content/uploads/2025/10/sample.csv"
PLIK_URL_404 = "https://httpstat.us/404"
PLIK_URL_503 = "https://httpstat.us/503"


class DownloadError(Exception):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        print("--- Jestem w klasie dla wyjątków związanych z pobieraniem plików. ---")
    pass

class NotFoundError(DownloadError):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        print("--- Jestem w klasie dla wyjątków związanych z kodem 404. ---")
    pass

class AccessDeniedError(DownloadError):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        print("--- Jestem w klasie dla wyjątków związanych z kodem 503. ---")
    pass


def log_execution_time(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        
        if args and hasattr(args[0], '__class__'):
            func_display_name = f"{args[0].__class__.__name__}.{func.__name__}"
        else:
            func_display_name = func.__name__

        print(f"\n[LOG CZASU] Rozpoczynam wykonanie: {func_display_name}...")
        start_time = time.time()

        result = func(*args, **kwargs)
        
        end_time = time.time()
        duration = end_time - start_time
        
        print(f"[LOG CZASU] Zakończono wykonanie: {func_display_name}. Czas trwania: {duration:.4f} s.")

        return result
    return wrapper

def download_file(url: str, filename: Optional[str] = "latest.csv") -> str:
    if not url:
        raise ValueError("Adres URL nie może być pusty.")

    if not filename.lower().endswith(".csv"):
        final_filename = filename + ".csv"
    else:
        final_filename = filename

    print(f"\nPobieranie pliku z: {url}")
    print(f"\nZapisywanie jako: {final_filename}")

    try:
        response = requests.get(url, stream=True)
        response.raise_for_status() 

        with open(final_filename, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    file.write(chunk)

        print(f"\nPlik został zapisany jako: '{final_filename}'.")
        return final_filename

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

class ETLProcessor:
    def __init__(self, source_filename: str):
        self.source_filename = source_filename
        print(f"\n--- Inicjuję proces ETL dla pliku: {self.source_filename} ---")

    def _extract(self) -> Generator[str, None, None]:
        print("Rozpoczynam czytanie pliku...")
        try:
            with open(self.source_filename, 'r', encoding='utf-8') as f:
                
                for line in f:
                    cleaned_line = line.strip()
                    if cleaned_line:
                        yield cleaned_line
        except FileNotFoundError:
            print(f"BŁĄD ETL: plik {self.source_filename} nie został znaleziony.")
            return
        except Exception as e:
            print(f"BŁĄD ETL: podczas odczytu pliku {self.source_filename}: {e}")
            return 

    def _transform(self, lines_generator: Generator[str, None, None]) -> Generator[Tuple, None, None]:
        print("Rozpoczynam przetwarzanie danych...")
        for line in lines_generator:
            try:
                parts = line.split(',')
                
                if len(parts) != 8:
                    print(f"Pominięto linię (nieprawidłowa liczba kolumn): {line}")
                    continue
                
                numer_porzadkowy = parts[0]
                data_values = parts[1:]
                
                numbers = [
                    float(v) 
                    for v in data_values 
                    if v.strip() != '-'
                ]
                
                missing_indices = [
                    i + 1 
                    for i, v in enumerate(data_values) 
                    if v.strip() == '-'
                ]
                
                sum_val = sum(numbers)
                avg_val = (sum_val / len(numbers)) if numbers else 0.0
                
                values_data = (numer_porzadkowy, sum_val, avg_val)
                missing_data = (numer_porzadkowy, missing_indices)
                
                yield values_data, missing_data

            except ValueError as e:
                print(f"Pominięto linię (błąd konwersji liczby): {line} - {e}")
            except Exception as e:
                print(f"Pominięto linię (nieznany błąd transformacji): {line} - {e}")

    def _load(self, 
              transformed_data_gen: Generator[Tuple, None, None], 
              values_file: str, 
              missing_file: str):
        
        print(f"Zapis danych do pliku {values_file} i {missing_file}...")
        
        try:
            with open(values_file, 'w', newline='', encoding='utf-8') as vf, \
                 open(missing_file, 'w', newline='', encoding='utf-8') as mf:
                
                values_writer = csv.writer(vf)
                missing_writer = csv.writer(mf)
                
                values_writer.writerow(["numer porządkowy", "suma", "średnia"])
                missing_writer.writerow(["numer porządkowy", "indeksy brakujących kolumn"])
                
                for values_data, missing_data in transformed_data_gen:
                    
                    values_writer.writerow([
                        values_data[0], 
                        f"{values_data[1]:.3f}", 
                        f"{values_data[2]:.3f}"
                    ])
                    
                    missing_indices_str = ", ".join(map(str, missing_data[1]))
                    missing_writer.writerow([missing_data[0], missing_indices_str])
                        
            print(f"Zakończono zapis. Utworzono pliki: {values_file} i {missing_file}.")
            
        except IOError as e:
            print(f"BŁĄD: Nie można zapisać do plików wyjściowych: {e}")
        except Exception as e:
            print(f"Nieznany błąd podczas zapisu ETL: {e}")

    @log_execution_time
    def run(self, values_file: str = "values.csv", missing_file: str = "missing_values.csv"):

        print("Uruchamiam pełen proces ETL...")
        
        lines_gen = self._extract()
        
        transformed_gen = self._transform(lines_gen)
        
        self._load(transformed_gen, values_file, missing_file)
        
        print("Proces ETL zakończony.")

if __name__ == "__main__":
    print("--- Program do pobierania plików CSV z niestandardową obsługą błędów ---")
    user_filename = input("Podaj nazwę pliku do zapisu (puste pole to 'latest.csv'): ")
    cleaned_filename = user_filename.strip()
    
    main_file_name = cleaned_filename if cleaned_filename else "test_sukces"
    
    urls_to_test = [
        (PLIK_URL, main_file_name),
        (PLIK_URL_404, "test_404"),
        (PLIK_URL_503, "test_503")
    ]
    
    successful_download_file = None
    
    for url, name in urls_to_test:
        try:
            print("\n" + "="*50)
            saved_filename = download_file(url, name)
            if url == PLIK_URL:
                successful_download_file = saved_filename
                
        except NotFoundError as e:
            print(f"\nBŁĄD: Plik nie istnieje! ({e})")
        except AccessDeniedError as e:
            print(f"\nBŁĄD: Dostęp zabroniony! ({e})")
        except DownloadError as e:
            print(f"\nBŁĄD: Błędne pobieranie danych! ({e}")
        except Exception as e:
            print(f"\nNieznany błąd! ({e})")

    if successful_download_file:
        print("\n" + "#"*50)
        print(f"Uruchamianie procesu ETL dla pliku: {successful_download_file}")
        try:
            processor = ETLProcessor(successful_download_file)
            processor.run() 
        except Exception as e:
            print(f"\nBŁĄD: Wystąpił krytyczny błąd podczas procesu ETL: {e}")
    else:
        print("\n" + "#"*50)
        print("Nie udało się pobrać głównego pliku. Proces ETL nie zostanie uruchomiony.")

    print("\n--- KONIEC ---")
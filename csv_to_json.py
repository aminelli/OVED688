#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Modulo per convertire file CSV in formato JSON
"""

import csv
import json
import argparse
from pathlib import Path
from typing import List, Dict


def read_csv_to_dict(csv_file: str, delimiter: str = ',', encoding: str = 'utf-8') -> List[Dict]:
    """
    Legge un file CSV e lo converte in una lista di dizionari.
    
    Args:
        csv_file: Percorso del file CSV
        delimiter: Delimitatore del CSV (default: ',')
        encoding: Encoding del file (default: 'utf-8')
    
    Returns:
        Lista di dizionari contenente i dati del CSV
    """
    data = []
    
    try:
        with open(csv_file, 'r', encoding=encoding) as file:
            csv_reader = csv.DictReader(file, delimiter=delimiter)
            for row in csv_reader:
                data.append(dict(row))
        
        print(f"Letti {len(data)} record dal file {csv_file}")
        return data
    
    except FileNotFoundError:
        print(f"Errore: File {csv_file} non trovato")
        return []
    except Exception as e:
        print(f"Errore durante la lettura del CSV: {e}")
        return []


def save_to_json(data: List[Dict], output_file: str, indent: int = 2, encoding: str = 'utf-8') -> bool:
    """
    Salva i dati in formato JSON.
    
    Args:
        data: Lista di dizionari da salvare
        output_file: Percorso del file JSON di output
        indent: Indentazione del JSON (default: 2)
        encoding: Encoding del file (default: 'utf-8')
    
    Returns:
        True se il salvataggio è riuscito, False altrimenti
    """
    try:
        with open(output_file, 'w', encoding=encoding) as file:
            json.dump(data, file, indent=indent, ensure_ascii=False)
        
        print(f"Dati salvati in {output_file}")
        return True
    
    except Exception as e:
        print(f"Errore durante il salvataggio del JSON: {e}")
        return False


def csv_to_json(csv_file: str, output_file: str = None, delimiter: str = ',', 
                indent: int = 2, print_output: bool = False) -> List[Dict]:
    """
    Converte un file CSV in formato JSON.
    
    Args:
        csv_file: Percorso del file CSV di input
        output_file: Percorso del file JSON di output (opzionale)
        delimiter: Delimitatore del CSV (default: ',')
        indent: Indentazione del JSON (default: 2)
        print_output: Se True, stampa il JSON su stdout (default: False)
    
    Returns:
        Lista di dizionari contenente i dati convertiti
    """
    # Legge il CSV
    data = read_csv_to_dict(csv_file, delimiter=delimiter)
    
    if not data:
        return []
    
    # Salva in file JSON se specificato
    if output_file:
        save_to_json(data, output_file, indent=indent)
    
    # Stampa su stdout se richiesto
    if print_output:
        print("\nJSON Output:")
        print(json.dumps(data, indent=indent, ensure_ascii=False))
    
    return data


def main():
    
    
    """Funzione principale per l'esecuzione da riga di comando."""
    parser = argparse.ArgumentParser(
        description='Converte file CSV in formato JSON',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
        Esempi d'uso:
        python csv_to_json.py input.csv -o output.json
        python csv_to_json.py input.csv --delimiter ";" --print
        python csv_to_json.py input.csv -o output.json --indent 4
        """
    )
    
    parser.add_argument('csv_file', help='File CSV di input')
    parser.add_argument('-o', '--output', help='File JSON di output')
    parser.add_argument('-d', '--delimiter', default=',', help='Delimitatore del CSV (default: ",")')
    parser.add_argument('-i', '--indent', type=int, default=2, help='Indentazione JSON (default: 2)')
    parser.add_argument('-p', '--print', action='store_true', dest='print_output',
                        help='Stampa il JSON su stdout')
    parser.add_argument('-e', '--encoding', default='utf-8', help='Encoding del file (default: utf-8)')
    
    args = parser.parse_args()
    
    # Verifica che il file CSV esista
    if not Path(args.csv_file).exists():
        print(f"Errore: Il file {args.csv_file} non esiste")
        return
    
    # Esegue la conversione
    data = csv_to_json(
        csv_file=args.csv_file,
        output_file=args.output,
        delimiter=args.delimiter,
        indent=args.indent,
        print_output=args.print_output
    )
    
    if data:
        print(f"\nConversione completata: {len(data)} record processati")


if __name__ == '__main__':
    main()

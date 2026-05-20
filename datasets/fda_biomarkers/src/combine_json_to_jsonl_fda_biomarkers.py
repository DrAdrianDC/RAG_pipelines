#!/usr/bin/env python3
"""
Script para combinar archivos JSON de FDA biomarkers en formato JSONL.

Este script:
1. Busca todos los archivos JSON en processed_json/ (formato FDA biomarkers)
2. Lee cada archivo JSON (objetos individuales)
3. Transforma cada objeto agregando campos requeridos (content, source, url, version)
4. Convierte cada objeto a una línea en formato JSONL
5. Genera fda_biomarkers.jsonl con todos los documentos

Formato JSONL: Cada línea es un objeto JSON válido, separado por saltos de línea.
Formato biomarkers esperado: url, nda_date, drug_name, biomarker, therapeutic_area, labeling_sections, Corpus
"""

import json
import os
import re
from pathlib import Path
from typing import List, Dict, Any, Union
import argparse
from collections import defaultdict
from datetime import datetime


def load_json_file(file_path: Path) -> List[Dict[str, Any]]:
    """
    Carga un archivo JSON y retorna una lista de objetos.
    
    Maneja dos casos:
    - Si el JSON es un objeto único: lo convierte en una lista con un elemento
    - Si el JSON es un array: retorna todos los objetos del array
    
    Args:
        file_path: Ruta al archivo JSON
        
    Returns:
        Lista de diccionarios (objetos JSON)
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Si es un objeto único (dict), lo convertimos a lista
        if isinstance(data, dict):
            return [data]
        # Si es un array, lo retornamos tal cual
        elif isinstance(data, list):
            return data
        else:
            print(f"⚠️  Advertencia: {file_path} contiene un tipo inesperado: {type(data)}")
            return []
    except json.JSONDecodeError as e:
        print(f"❌ Error al parsear JSON en {file_path}: {e}")
        return []
    except Exception as e:
        print(f"❌ Error al leer {file_path}: {e}")
        return []


def transform_to_azure_format(obj: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transforma un objeto JSON de FDA biomarkers al formato esperado por Azure AI Search.
    
    Campos requeridos por Azure:
    - content: El contenido principal del documento (preservado del JSON original)
    - source: Nombre de la fuente ("fda_biomarkers")
    - url: URL del documento (preservado del JSON original)
    - version: Versión del documento ("2.0")
    
    Además, preserva todos los demás campos del JSON original como metadata.
    
    Args:
        obj: Objeto JSON original con campos: url, content, nda_date, drug_name, biomarker, etc.
        
    Returns:
        Objeto transformado con campos principales y metadata preservada
    """
    transformed = {}
    
    # Campos principales requeridos por Azure (content ya existe, solo agregamos los demás)
    transformed["content"] = obj.get("content", "")
    transformed["source"] = "fda_biomarkers"
    transformed["url"] = obj.get("url", "")
    transformed["version"] = "2.0"
    
    # Copiar todos los demás campos del JSON original (metadata)
    for key, value in obj.items():
        if key != "content":  # Ya lo agregamos arriba
            transformed[key] = value
    
    return transformed


def find_json_files_in_directory(directory: str, recursive: bool = True) -> List[Path]:
    """
    Encuentra todos los archivos JSON en un directorio específico.
    
    Args:
        directory: Ruta del directorio a buscar
        recursive: Si True, busca recursivamente en subdirectorios
        
    Returns:
        Lista de rutas a archivos JSON encontrados
    """
    json_files = []
    dir_path = Path(directory)
    
    if not dir_path.exists():
        print(f"⚠️  Advertencia: El directorio {directory} no existe")
        return []
    
    if recursive:
        # Buscar recursivamente
        json_files.extend(dir_path.rglob("*.json"))
    else:
        # Solo en el directorio actual
        json_files.extend(dir_path.glob("*.json"))
    
    return sorted(json_files)


def combine_json_to_jsonl(
    input_directory: str,
    output_file: str,
    recursive: bool = True,
    add_source_info: bool = False,
    transform_for_azure: bool = True
) -> Dict[str, Any]:
    """
    Combina archivos JSON de un directorio en un archivo JSONL.
    
    Args:
        input_directory: Directorio donde buscar archivos JSON
        output_file: Ruta del archivo JSONL de salida
        recursive: Si True, busca recursivamente en subdirectorios
        add_source_info: Si True, agrega información del archivo fuente a cada objeto
        transform_for_azure: Si True, transforma los campos al formato esperado por Azure
        
    Returns:
        Diccionario con estadísticas del proceso
    """
    print(f"\n🔍 Procesando directorio: {input_directory}")
    json_files = find_json_files_in_directory(input_directory, recursive=recursive)
    
    if not json_files:
        print(f"⚠️  No se encontraron archivos JSON en {input_directory}")
        return {
            "total_files": 0,
            "total_objects": 0,
            "errors": 0,
            "files_processed": 0
        }
    
    print(f"📁 Encontrados {len(json_files)} archivos JSON")
    
    if transform_for_azure:
        print(f"🔄 Transformando campos al formato Azure (source: fda_biomarkers)")
    
    # Estadísticas
    stats = {
        "total_files": len(json_files),
        "total_objects": 0,
        "errors": 0,
        "files_processed": 0,
        "objects_per_file": defaultdict(int)
    }
    
    # Crear directorio de salida si no existe
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    print(f"📝 Escribiendo en: {output_file}")
    print("-" * 60)
    
    # Procesar cada archivo JSON
    with open(output_file, 'w', encoding='utf-8') as outfile:
        for idx, json_file in enumerate(json_files, 1):
            try:
                # Cargar objetos del archivo
                objects = load_json_file(json_file)
                
                if not objects:
                    stats["errors"] += 1
                    continue
                
                # Escribir cada objeto como una línea JSONL
                for obj in objects:
                    # Transformar al formato Azure si está habilitado
                    if transform_for_azure:
                        obj = transform_to_azure_format(obj)
                    
                    # Opcionalmente agregar información del archivo fuente
                    if add_source_info:
                        obj["_source_file"] = str(json_file)
                        obj["_source_relative"] = str(json_file.relative_to(Path.cwd()))
                    
                    # Escribir como una línea JSON (sin espacios, compacto)
                    json_line = json.dumps(obj, ensure_ascii=False)
                    outfile.write(json_line + '\n')
                    
                    stats["total_objects"] += 1
                    stats["objects_per_file"][json_file.name] += 1
                
                stats["files_processed"] += 1
                
                # Mostrar progreso cada 10 archivos
                if idx % 10 == 0:
                    print(f"📊 Procesados {idx}/{len(json_files)} archivos... "
                          f"({stats['total_objects']} objetos hasta ahora)")
                
            except Exception as e:
                print(f"❌ Error procesando {json_file}: {e}")
                stats["errors"] += 1
    
    print("-" * 60)
    print(f"✅ Directorio {input_directory} completado!")
    print(f"📊 Estadísticas:")
    print(f"   - Archivos procesados: {stats['files_processed']}/{stats['total_files']}")
    print(f"   - Total de objetos escritos: {stats['total_objects']}")
    print(f"   - Errores: {stats['errors']}")
    print(f"   - Archivo de salida: {output_file}")
    if output_path.exists():
        print(f"   - Tamaño del archivo: {output_path.stat().st_size / (1024*1024):.2f} MB")
    
    return stats


def process_multiple_directories(
    input_directories: List[str],
    output_dir: str = "Output",
    recursive: bool = True,
    add_source_info: bool = False,
    transform_for_azure: bool = True
) -> Dict[str, Any]:
    """
    Procesa múltiples directorios y genera un JSONL separado para cada uno.
    
    Args:
        input_directories: Lista de directorios a procesar
        output_dir: Directorio donde guardar los archivos JSONL
        recursive: Si True, busca recursivamente en subdirectorios
        add_source_info: Si True, agrega información del archivo fuente a cada objeto
        transform_for_azure: Si True, transforma los campos al formato esperado por Azure
        
    Returns:
        Diccionario con estadísticas generales
    """
    print("=" * 70)
    print("🔧 Combinador de JSON a JSONL - drugs_fda para Azure AI Search")
    print("=" * 70)
    print()
    print(f"📂 Directorios a procesar: {len(input_directories)}")
    for dir_path in input_directories:
        print(f"   - {dir_path}")
    print()
    print(f"📁 Directorio de salida: {output_dir}")
    print("=" * 70)
    
    # Crear directorio de salida
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Estadísticas generales
    all_stats = {
        "directories_processed": 0,
        "total_files": 0,
        "total_objects": 0,
        "total_errors": 0,
        "output_files": []
    }
    
    # Procesar cada directorio por separado
    for directory in input_directories:
        dir_path = Path(directory)
        
        # Generar nombre del archivo de salida basado en el nombre del directorio
        # Ejemplo: "drugs_fda/processed-json" -> "drugs_fda.jsonl"
        
        # Si el directorio es "processed-json", usar el nombre del directorio padre
        if dir_path.name == "processed-json":
            output_filename = f"{dir_path.parent.name}.jsonl"
        else:
            # Usar el nombre del directorio mismo
            output_filename = f"{dir_path.name}.jsonl"
        
        # Normalizar nombre (reemplazar @ y - por _)
        output_filename = output_filename.replace("@", "_").replace("-", "_")
        
        output_file = output_path / output_filename
        
        # Procesar este directorio
        stats = combine_json_to_jsonl(
            input_directory=directory,
            output_file=str(output_file),
            recursive=recursive,
            add_source_info=add_source_info,
            transform_for_azure=transform_for_azure
        )
        
        # Acumular estadísticas
        all_stats["directories_processed"] += 1
        all_stats["total_files"] += stats["total_files"]
        all_stats["total_objects"] += stats["total_objects"]
        all_stats["total_errors"] += stats["errors"]
        all_stats["output_files"].append(str(output_file))
    
    # Resumen final
    print()
    print("=" * 70)
    print("🎉 Proceso completado para todos los directorios!")
    print("=" * 70)
    print(f"📊 Resumen general:")
    print(f"   - Directorios procesados: {all_stats['directories_processed']}")
    print(f"   - Total de archivos JSON: {all_stats['total_files']}")
    print(f"   - Total de objetos escritos: {all_stats['total_objects']}")
    print(f"   - Total de errores: {all_stats['total_errors']}")
    print()
    print("📄 Archivos JSONL generados:")
    for output_file in all_stats["output_files"]:
        file_path = Path(output_file)
        if file_path.exists():
            size_mb = file_path.stat().st_size / (1024*1024)
            print(f"   ✅ {output_file} ({size_mb:.2f} MB)")
        else:
            print(f"   ⚠️  {output_file} (no generado)")
    
    return all_stats


def main():
    """Función principal con argumentos de línea de comandos"""
    parser = argparse.ArgumentParser(
        description="Combina archivos JSON de drugs_fda en formato JSONL para Azure AI Search",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos de uso:

  # Procesar directorio de drugs_fda (genera drugs_fda.jsonl)
  python combine_drugs_fda_to_jsonl.py -d drugs_fda/processed-json

  # Especificar directorio de salida personalizado
  python combine_drugs_fda_to_jsonl.py -d drugs_fda/processed-json -o mi_output

  # Incluir información del archivo fuente
  python combine_drugs_fda_to_jsonl.py -d drugs_fda/processed-json --add-source

  # Solo buscar en directorio principal (no recursivo)
  python combine_drugs_fda_to_jsonl.py -d drugs_fda/processed-json --no-recursive
        """
    )
    
    parser.add_argument(
        '-d', '--directories',
        nargs='+',
        required=True,
        help='Directorios donde buscar archivos JSON (genera un JSONL por directorio)'
    )
    
    parser.add_argument(
        '-o', '--output-dir',
        default='Output',
        help='Directorio donde guardar los archivos JSONL (default: Output)'
    )
    
    parser.add_argument(
        '--no-recursive',
        action='store_true',
        help='No buscar recursivamente en subdirectorios'
    )
    
    parser.add_argument(
        '--add-source',
        action='store_true',
        help='Agregar campos _source_file y _source_relative a cada objeto'
    )
    
    parser.add_argument(
        '--no-transform',
        action='store_true',
        help='No transformar campos al formato Azure (preservar campos originales)'
    )
    
    args = parser.parse_args()
    
    # Ejecutar el proceso
    stats = process_multiple_directories(
        input_directories=args.directories,
        output_dir=args.output_dir,
        recursive=not args.no_recursive,
        add_source_info=args.add_source,
        transform_for_azure=not args.no_transform
    )
    
    return 0 if stats["total_errors"] == 0 else 1


# Ejemplo de uso directo (sin argumentos de línea de comandos)
if __name__ == "__main__":
    import sys
    
    # Si se ejecuta sin argumentos, usar configuración por defecto para biomarkers
    if len(sys.argv) == 1:
        # Obtener el directorio donde está el script
        script_dir = Path(__file__).parent.absolute()
        
        # Input: processed_json/ en el mismo directorio del script
        input_dir = script_dir / "processed_json"
        output_file = script_dir / "fda_biomarkers.jsonl"
        
        # Verificar que el directorio existe
        if not input_dir.exists():
            print(f"❌ ERROR: El directorio {input_dir} no existe")
            print(f"   Asegúrate de ejecutar primero json_split_and_clean_fda_biomarkers.py")
            sys.exit(1)
        
        # Procesar con transformación habilitada
        stats = combine_json_to_jsonl(
            input_directory=str(input_dir),
            output_file=str(output_file),
            recursive=False,  # Solo archivos directos en processed_json/
            add_source_info=False,
            transform_for_azure=True
        )
        
        print(f"\n✅ JSONL generado: {output_file}")
    else:
        # Ejecutar con argumentos de línea de comandos
        sys.exit(main())


#!/usr/bin/env python3
"""
Script para reorganizar el contenido de output.json eliminando solapamientos
entre registros causados por errores de salto de página en la extracción.
"""

import json
import re
from typing import List, Dict, Optional, Tuple


def normalize_section_name(section: str) -> str:
    """
    Normaliza el nombre de una sección para búsqueda.
    Convierte a mayúsculas y elimina espacios extras.
    """
    return re.sub(r'\s+', ' ', section.strip().upper())


def generate_section_patterns(section_name: str) -> List[str]:
    """
    Genera patrones de búsqueda para encontrar el encabezado de una sección
    en el contenido. Considera variaciones comunes:
    - Con números: "1 INDICATIONS AND USAGE"
    - Sin números: "INDICATIONS AND USAGE"
    - Con HTML: "<b>Indications and Usage</b>"
    - Minúsculas/mayúsculas variadas
    """
    normalized = normalize_section_name(section_name)
    patterns = []
    
    # Patrón exacto en mayúsculas
    patterns.append(normalized)
    
    # Con número al inicio (ej: "1 INDICATIONS AND USAGE")
    patterns.append(rf"\d+\s+{re.escape(normalized)}")
    
    # Con subsecciones (ej: "1.1 INDICATIONS AND USAGE")
    patterns.append(rf"\d+\.\d+\s+{re.escape(normalized)}")
    patterns.append(rf"\d+\.\d+\.\d+\s+{re.escape(normalized)}")
    
    # Con HTML tags (ej: "<b>Indications and Usage</b>")
    html_escaped = re.escape(normalized)
    patterns.append(rf"<[^>]*>{html_escaped}</[^>]*>")
    patterns.append(rf"<[^>]*>{html_escaped}")
    
    # Con formato mixto (ej: "1. Indications and Usage")
    title_case = section_name.title()
    patterns.append(rf"\d+\s*\.\s*{re.escape(title_case)}")
    
    return patterns


def find_section_header_position(content: str, section_name: str) -> Optional[int]:
    """
    Busca la posición del encabezado de una sección en el contenido.
    Retorna la posición del inicio del encabezado o None si no se encuentra.
    """
    patterns = generate_section_patterns(section_name)
    
    # Buscar el primer patrón que coincida, pero verificar que parece un encabezado real
    best_match = None
    best_pos = len(content)
    
    for pattern in patterns:
        matches = list(re.finditer(pattern, content, re.IGNORECASE | re.MULTILINE))
        for match in matches:
            pos = match.start()
            # Verificar que parece un encabezado (al inicio de línea o después de número)
            line_start = content.rfind('\n', 0, pos) + 1
            line_end = content.find('\n', pos)
            if line_end == -1:
                line_end = len(content)
            
            line_text = content[line_start:line_end].strip()
            
            # Verificar que la línea parece un encabezado:
            # - Empieza con número seguido de espacio
            # - O es solo el texto del encabezado (sin mucho más texto después)
            # - O está en mayúsculas principalmente
            
            is_header = False
            
            # Caso 1: Empieza con número (ej: "1 INDICATIONS AND USAGE")
            if re.match(r'^\d+\s+', line_text):
                is_header = True
            # Caso 2: Es básicamente solo el encabezado (pocas palabras adicionales)
            elif len(line_text) < len(normalize_section_name(section_name)) * 1.5:
                is_header = True
            # Caso 3: Está principalmente en mayúsculas y empieza con el texto
            elif line_text.upper().startswith(normalize_section_name(section_name)):
                upper_ratio = sum(1 for c in line_text if c.isupper()) / max(len(line_text), 1)
                if upper_ratio > 0.6:  # Más del 60% en mayúsculas
                    is_header = True
            
            if is_header and pos < best_pos:
                best_match = match
                best_pos = pos
    
    if best_match:
        return best_match.start()
    
    # Si no se encuentra con los patrones estándar, buscar texto aproximado
    # (útil para casos donde el formato varía mucho)
    normalized = normalize_section_name(section_name)
    words = normalized.split()
    
    # Buscar todas las palabras del encabezado en secuencia
    if len(words) >= 2:
        # Buscar secuencia de todas las palabras principales
        word_pattern = r'\s+'.join([re.escape(word) for word in words[:min(3, len(words))]])
        matches = list(re.finditer(word_pattern, content, re.IGNORECASE))
        
        for match in matches:
            pos = match.start()
            line_start = content.rfind('\n', 0, pos) + 1
            line_end = content.find('\n', pos)
            if line_end == -1:
                line_end = len(content)
            
            line_text = content[line_start:line_end].strip()
            
            # Verificar que parece un encabezado
            if (re.match(r'^\d+\s+', line_text) or 
                len(line_text) < len(normalized) * 1.5 or
                (line_text.upper().startswith(words[0]) and 
                 sum(1 for c in line_text if c.isupper()) / max(len(line_text), 1) > 0.6)):
                return pos
    
    return None


def clean_html_tags(text: str) -> str:
    """
    Cierra etiquetas HTML que puedan haberse quedado abiertas al hacer el corte.
    Solo cierra las etiquetas comunes si están abiertas al final del texto.
    """
    # No intentamos cerrar todas las etiquetas automáticamente,
    # solo limpiamos espacios en blanco
    return text.strip()


def fix_html_fragments(text: str) -> str:
    """
    Intenta reparar etiquetas HTML fragmentadas al inicio o final del texto.
    Si encuentra etiquetas abiertas al inicio sin cerrar, las cierra.
    Si encuentra etiquetas de cierre sin abrir al final, las elimina.
    """
    # Etiquetas comunes que pueden estar fragmentadas
    common_tags = ['p', 'b', 'i', 'u', 'strong', 'em', 'div', 'span']
    
    # Cerrar etiquetas abiertas al inicio
    for tag in common_tags:
        # Si empieza con <tag> o <tag (sin cerrar)
        if re.match(rf'^<{tag}[^>]*>', text, re.IGNORECASE):
            # Contar cuántas se abren sin cerrar
            opens = len(re.findall(rf'<{tag}[^>]*>', text, re.IGNORECASE))
            closes = len(re.findall(rf'</{tag}>', text, re.IGNORECASE))
            if opens > closes:
                # Cerrar las que faltan
                for _ in range(opens - closes):
                    text += f'</{tag}>'
        
        # Si termina con </tag> sin abrir correspondiente
        if text.endswith(f'</{tag}>'):
            opens = len(re.findall(rf'<{tag}[^>]*>', text, re.IGNORECASE))
            closes = len(re.findall(rf'</{tag}>', text, re.IGNORECASE))
            if closes > opens:
                # Eliminar cierres extras
                for _ in range(closes - opens):
                    text = text[:-len(f'</{tag}>')]
    
    return text


def normalize_content(content: str) -> str:
    """
    Normaliza el contenido: limpia espacios en blanco y repara HTML.
    """
    # Limpiar espacios en blanco al principio y final
    content = content.strip()
    
    # Normalizar múltiples saltos de línea
    content = re.sub(r'\n{3,}', '\n\n', content)
    
    # Reparar HTML fragmentado
    content = fix_html_fragments(content)
    
    return content


def deduplicate_sections(sections: List[str]) -> List[str]:
    """
    Elimina secciones duplicadas manteniendo el orden.
    """
    seen = set()
    result = []
    for section in sections:
        normalized = normalize_section_name(section)
        if normalized not in seen:
            seen.add(normalized)
            result.append(section)
    return result


def reorganize_records(records: List[Dict]) -> List[Dict]:
    """
    Reorganiza los registros moviendo texto residual al registro anterior.
    """
    reorganized = []
    
    for i, record in enumerate(records):
        # Copiar el registro
        new_record = record.copy()
        
        # Obtener la primera sección del labeling_sections
        if not record.get('labeling_sections'):
            # Si no hay secciones, mantener el contenido tal cual
            new_record['content'] = normalize_content(record.get('content', ''))
            reorganized.append(new_record)
            continue
        
        first_section = record['labeling_sections'][0]
        content = record.get('content', '')
        
        # Buscar la posición del encabezado de la primera sección
        header_pos = find_section_header_position(content, first_section)
        
        if header_pos is not None:
            if header_pos > 0:
                # Hay texto antes del encabezado (residuo del registro anterior)
                residue = content[:header_pos].strip()
                actual_content = content[header_pos:].strip()
                
                # Si hay registro anterior, agregar el residuo al final
                if i > 0 and reorganized:
                    previous_content = reorganized[-1].get('content', '')
                    # Agregar el residuo con un salto de línea si es necesario
                    if previous_content and not previous_content.endswith('\n'):
                        reorganized[-1]['content'] = previous_content + '\n' + residue
                    else:
                        reorganized[-1]['content'] = previous_content + residue
                    # Normalizar el contenido del registro anterior
                    reorganized[-1]['content'] = normalize_content(reorganized[-1]['content'])
                
                # Actualizar el contenido del registro actual
                new_record['content'] = normalize_content(actual_content)
            else:
                # El encabezado está al inicio, no hay residuo
                new_record['content'] = normalize_content(content)
        else:
            # No se encontró el encabezado de la primera sección
            # Esto significa que todo el contenido es residual del registro anterior
            residue = content.strip()
            
            # Si hay registro anterior, agregar todo el contenido al final
            if i > 0 and reorganized and residue:
                previous_content = reorganized[-1].get('content', '')
                # Agregar el residuo con un salto de línea si es necesario
                if previous_content and not previous_content.endswith('\n'):
                    reorganized[-1]['content'] = previous_content + '\n' + residue
                else:
                    reorganized[-1]['content'] = previous_content + residue
                # Normalizar el contenido del registro anterior
                reorganized[-1]['content'] = normalize_content(reorganized[-1]['content'])
                # El registro actual queda vacío ya que todo el contenido fue residual
                new_record['content'] = ''
            else:
                # No hay registro anterior o no hay residuo, mantener el contenido normalizado
                # (esto puede pasar en el primer registro o si el encabezado simplemente no está presente)
                new_record['content'] = normalize_content(content)
        
        # Deduplicar labeling_sections
        if 'labeling_sections' in new_record:
            new_record['labeling_sections'] = deduplicate_sections(new_record['labeling_sections'])
        
        reorganized.append(new_record)
    
    return reorganized


def main():
    """
    Función principal que procesa output.json y genera final_output.json
    """
    input_file = 'output.json'
    output_file = 'final_output.json'
    
    print(f"Leyendo {input_file}...")
    with open(input_file, 'r', encoding='utf-8') as f:
        records = json.load(f)
    
    print(f"Procesando {len(records)} registros...")
    
    # Reorganizar los registros
    reorganized_records = reorganize_records(records)
    
    print(f"Guardando resultados en {output_file}...")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(reorganized_records, f, ensure_ascii=False, indent=2)
    
    print(f"✓ Proceso completado. {len(reorganized_records)} registros procesados.")
    
    # Estadísticas
    moved_count = 0
    for i in range(1, len(reorganized_records)):
        original_content = records[i].get('content', '')
        new_content = reorganized_records[i].get('content', '')
        if len(new_content) < len(original_content):
            moved_count += 1
    
    print(f"  - {moved_count} registros tuvieron contenido movido al registro anterior")


if __name__ == '__main__':
    main()


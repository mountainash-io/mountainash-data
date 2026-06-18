#!/usr/bin/env python3
"""
Enrich learning graphs, retrofit concept markers, and create refresh-state.json
for 4 mountainash textbook projects.
"""

import json
import re
import hashlib
import os
from datetime import datetime, timezone
from pathlib import Path
from difflib import SequenceMatcher

BASE = Path("/home/nathanielramm/git/mountainash-io/mountainash/mountainash-central")

PROJECTS = [
    {
        "name": "mountainash-treespec",
        "profile_dir": "03.profile/mountainash-treespec",
    },
    {
        "name": "mountainash-transport",
        "profile_dir": "03.profile/mountainash-transport",
    },
    {
        "name": "mountainash-utils-secrets",
        "profile_dir": "03.profile/mountainash-utils-secrets",
    },
    {
        "name": "mountainash-wearables",
        "profile_dir": "13.profile-usecases/mountainash-wearables",
    },
]


def normalize(s):
    """Normalize a string for fuzzy matching."""
    return re.sub(r'[^a-z0-9]', '', s.lower())


def load_json(path):
    with open(path) as f:
        return json.load(f)


def save_json(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        f.write('\n')


def load_modules(profile_dir):
    """Load all module profiles and build symbol lookup tables."""
    manifest = load_json(profile_dir / "manifest.json")
    source_commit = manifest.get("source", {}).get("git", {}).get("current_hash", "")

    modules = {}
    symbol_to_module = {}

    module_files = manifest.get("profile_files", {}).get("modules", {})
    for mod_id, mod_file in module_files.items():
        mod_path = profile_dir / mod_file
        if not mod_path.exists():
            continue
        mod_data = load_json(mod_path)
        modules[mod_id] = mod_data

        mod_source_path = mod_data.get("path", "")
        public_api = mod_data.get("public_api", [])
        key_classes = [c["name"] if isinstance(c, dict) else c for c in mod_data.get("key_classes", []) if c]
        key_functions = []
        for kf in mod_data.get("key_functions", []):
            if isinstance(kf, dict):
                key_functions.append(kf.get("name", ""))
            elif isinstance(kf, str):
                key_functions.append(kf)

        all_symbols = set()
        for s in public_api:
            clean = re.sub(r'\s*\(.*?\)\s*', '', s).strip()
            if clean:
                all_symbols.add(clean)
        for s in key_classes:
            all_symbols.add(s)
        for s in key_functions:
            clean = re.sub(r'\s*\(.*?\)\s*', '', s).strip()
            if clean:
                all_symbols.add(clean)

        for sym in all_symbols:
            symbol_to_module[sym] = (mod_id, mod_source_path)

    return modules, symbol_to_module, source_commit, manifest


def match_concept_to_module(concept_label, symbol_to_module, modules, chapter_dir, is_foundation):
    """Match a concept label to a source module using heuristics."""
    if is_foundation:
        return None, None, 0.0

    label = concept_label
    label_norm = normalize(label)

    # Exact match
    for sym, (mod_id, mod_path) in symbol_to_module.items():
        if label == sym:
            return mod_id, mod_path, 1.0

    # Normalized exact match
    for sym, (mod_id, mod_path) in symbol_to_module.items():
        if label_norm == normalize(sym):
            return mod_id, mod_path, 0.8

    # Stripped suffix match
    stripped_label = re.sub(
        r'\s+(Class|Method|Protocol|Enum|Dataclass|Function|Map|Builder|'
        r'Handler|Pattern|Strategy|Mixin|Helper|Utility|Support|Model|'
        r'Module Overview|Overview)$', '', label)
    stripped_norm = normalize(stripped_label)

    for sym, (mod_id, mod_path) in symbol_to_module.items():
        sym_norm = normalize(sym)
        if stripped_norm == sym_norm:
            return mod_id, mod_path, 0.85

    # Containment match
    best_containment = None
    best_containment_len = 0
    for sym, (mod_id, mod_path) in symbol_to_module.items():
        sym_norm = normalize(sym)
        if len(sym_norm) >= 4:
            if sym_norm in label_norm or label_norm in sym_norm:
                if len(sym_norm) > best_containment_len:
                    best_containment = (mod_id, mod_path)
                    best_containment_len = len(sym_norm)

    if best_containment:
        return best_containment[0], best_containment[1], 0.7

    # Module-name match
    for mod_id, mod_data in modules.items():
        mod_path = mod_data.get("path", "")
        short_name = mod_id.split('.')[-1].replace('-', '').replace('_', '')
        if len(short_name) >= 4 and short_name in label_norm:
            return mod_id, mod_path, 0.6

    # Fuzzy sequence match
    best_ratio = 0
    best_match = None
    for sym, (mod_id, mod_path) in symbol_to_module.items():
        ratio = SequenceMatcher(None, label_norm, normalize(sym)).ratio()
        if ratio > best_ratio:
            best_ratio = ratio
            best_match = (mod_id, mod_path)

    if best_ratio >= 0.7:
        return best_match[0], best_match[1], round(best_ratio * 0.8, 2)

    return None, None, 0.3


def get_chapter_concepts(chapter_path):
    """Parse the Concepts Covered section from a chapter."""
    with open(chapter_path) as f:
        text = f.read()

    m = re.search(r'## Concepts Covered\n\n((?:- .+\n)+)', text)
    if not m:
        return []

    lines = m.group(1).strip().split('\n')
    return [l.lstrip('- ').strip() for l in lines if l.strip()]


def find_body_start(lines):
    """Find where the chapter body starts (after front matter and standard sections)."""
    start = 0
    if lines and lines[0].strip() == '---':
        for i in range(1, len(lines)):
            if lines[i].strip() == '---':
                start = i + 1
                break

    standard_sections = {'summary', 'concepts covered', 'learning graph ids',
                         'prerequisites', 'learning objectives', 'learning outcomes'}

    for i in range(start, len(lines)):
        line = lines[i].strip()
        if line.startswith('## '):
            heading = line[3:].strip().lower()
            if heading not in standard_sections:
                return i

    # Fallback: find --- separator
    for i in range(start, len(lines)):
        if lines[i].strip() == '---' and i > start + 5:
            return i + 1

    return start


def find_concept_position(lines, concept_label, body_start):
    """Find the best line to insert a concept marker.
    Returns line index or None.
    """
    label_lower = concept_label.lower()

    variations = [label_lower]
    for suffix in [' class', ' method', ' protocol', ' enum', ' dataclass',
                   ' function', ' pattern', ' strategy', ' mixin', ' handler',
                   ' builder', ' model', ' module overview', ' overview',
                   ' base', ' mode']:
        if label_lower.endswith(suffix):
            variations.append(label_lower[:-len(suffix)])
    # Strip common prefixes (e.g. "ChangeType PARAM_ADDED" -> "param_added")
    for prefix in ['changetype ', 'feel ', 'dmn ']:
        if label_lower.startswith(prefix):
            stripped = label_lower[len(prefix):]
            variations.append(stripped)
            variations.append(stripped.replace('_', ' '))
            variations.append(stripped.replace(' ', '_'))
    variations.append(label_lower.replace(' ', '-'))
    variations.append(label_lower.replace(' ', '_'))
    # Handle slash variants (e.g. "Add Remove" -> "add/remove")
    variations.append(label_lower.replace(' ', '/'))
    # For short labels (2-3 words), also add individual words >= 5 chars as search terms
    words = label_lower.split()
    if len(words) <= 3:
        for w in words:
            if len(w) >= 5 and w not in variations:
                variations.append(w)

    def norm_sep(s):
        """Normalize separators (hyphens, underscores, slashes) to spaces for comparison."""
        return re.sub(r'[-_/]', ' ', s)

    # Strategy 1: heading match
    for i in range(body_start, len(lines)):
        line = lines[i]
        if line.startswith('#'):
            heading_text = re.sub(r'^#+\s*', '', line).strip().lower()
            heading_norm = norm_sep(heading_text)
            for var in variations:
                var_norm = norm_sep(var)
                if var_norm == heading_norm or (len(var_norm) >= 5 and var_norm in heading_norm):
                    return i

    # Strategy 2: paragraph match (skip code blocks, details, admonitions)
    in_code_block = False
    in_details = 0
    in_admonition = False

    for i in range(body_start, len(lines)):
        line = lines[i]

        if line.strip().startswith('```'):
            in_code_block = not in_code_block
            continue
        if in_code_block:
            continue

        if '<details' in line:
            in_details += 1
            continue
        if '</details>' in line:
            in_details = max(0, in_details - 1)
            continue
        if in_details > 0:
            continue

        if line.strip().startswith('!!!') or line.strip().startswith('???'):
            in_admonition = True
            continue
        if in_admonition:
            if line.strip() and not line.startswith('    '):
                in_admonition = False
            else:
                continue

        line_lower = line.lower()
        line_norm = norm_sep(line_lower)
        for var in variations:
            var_norm = norm_sep(var)
            if len(var_norm) >= 4 and (var in line_lower or var_norm in line_norm) and len(line.strip()) > 10:
                # Walk back to paragraph start
                para_start = i
                for j in range(i - 1, body_start - 1, -1):
                    stripped = lines[j].strip()
                    if stripped == '' or lines[j].startswith('#') or stripped.startswith('<iframe') or stripped.startswith('|'):
                        para_start = j + 1 if not lines[j].startswith('#') else j
                        break
                return para_start

    return None


def retrofit_markers(chapter_path, concepts_with_ids):
    """Insert concept markers into a chapter file.
    Returns list of (concept_id, concept_label, placed: bool)
    """
    with open(chapter_path) as f:
        text = f.read()

    existing_markers = set(int(m) for m in re.findall(r'<!-- concept:(\d+) -->', text))

    lines = text.split('\n')
    body_start = find_body_start(lines)

    results = []
    insertions = {}

    for concept_id, concept_label in concepts_with_ids:
        if concept_id in existing_markers:
            results.append((concept_id, concept_label, True))
            continue

        pos = find_concept_position(lines, concept_label, body_start)
        if pos is not None:
            if pos not in insertions:
                insertions[pos] = []
            insertions[pos].append(f'<!-- concept:{concept_id} -->')
            results.append((concept_id, concept_label, True))
        else:
            results.append((concept_id, concept_label, False))

    if not insertions:
        return results

    new_lines = list(lines)
    for pos in sorted(insertions.keys(), reverse=True):
        for marker in reversed(insertions[pos]):
            new_lines.insert(pos, marker)

    with open(chapter_path, 'w') as f:
        f.write('\n'.join(new_lines))

    return results


def compute_file_hash(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            h.update(chunk)
    return h.hexdigest()


def count_words(path):
    with open(path) as f:
        text = f.read()
    text = re.sub(r'^---\n.*?\n---\n', '', text, flags=re.DOTALL)
    return len(text.split())


def process_project(project_config):
    name = project_config["name"]
    profile_dir = BASE / project_config["profile_dir"]
    textbook_dir = BASE / "06.textbook-sites" / name
    lg_dir = BASE / "05.learning-graph" / name
    chapters_dir = textbook_dir / "docs" / "chapters"

    print(f"\n{'='*60}")
    print(f"Processing: {name}")
    print(f"{'='*60}")

    lg_path = lg_dir / "learning-graph.json"
    lg_data = load_json(lg_path)

    concept_id_map = {}
    concept_by_id = {}
    for node in lg_data["nodes"]:
        concept_id_map[node["label"]] = node["id"]
        concept_by_id[node["id"]] = node

    modules, symbol_to_module, source_commit, manifest = load_modules(profile_dir)

    concept_to_chapter = {}
    chapter_dirs = sorted([d for d in chapters_dir.iterdir() if d.is_dir()])

    all_marker_results = []
    total_markers_placed = 0
    total_markers_failed = 0

    for ch_dir in chapter_dirs:
        ch_index = ch_dir / "index.md"
        if not ch_index.exists():
            continue

        ch_name = ch_dir.name
        concepts = get_chapter_concepts(ch_index)

        concepts_with_ids = []
        for c in concepts:
            cid = concept_id_map.get(c)
            if cid:
                concept_to_chapter[cid] = ch_name
                concepts_with_ids.append((cid, c))
            else:
                c_norm = normalize(c)
                for label, lid in concept_id_map.items():
                    if normalize(label) == c_norm:
                        concept_to_chapter[lid] = ch_name
                        concepts_with_ids.append((lid, c))
                        break

        results = retrofit_markers(ch_index, concepts_with_ids)
        placed = sum(1 for _, _, p in results if p)
        failed = sum(1 for _, _, p in results if not p)
        total_markers_placed += placed
        total_markers_failed += failed

        if failed > 0:
            for cid, clabel, p in results:
                if not p:
                    all_marker_results.append((ch_name, cid, clabel))

        print(f"  {ch_name}: {placed}/{len(results)} markers placed")

    # TASK B: Enrich learning graph
    is_foundation_chapter = lambda ch: ch is not None and ch.startswith("01-")
    low_confidence = []
    enriched_count = 0

    for node in lg_data["nodes"]:
        nid = node["id"]
        label = node["label"]
        chapter = concept_to_chapter.get(nid)
        is_found = is_foundation_chapter(chapter)

        mod_id, mod_path, confidence = match_concept_to_module(
            label, symbol_to_module, modules, chapter, is_found
        )

        node["source_module"] = mod_id
        node["source_path"] = mod_path
        node["chapter"] = chapter
        node["match_confidence"] = confidence
        enriched_count += 1

        if confidence < 0.6 and not is_found:
            low_confidence.append((nid, label, confidence, chapter))

    lg_data["metadata"]["source_commit"] = source_commit
    lg_data["metadata"]["profile_dir"] = project_config["profile_dir"]

    save_json(lg_path, lg_data)
    textbook_lg_path = textbook_dir / "docs" / "learning-graph" / "learning-graph.json"
    save_json(textbook_lg_path, lg_data)

    print(f"\n  Learning graph enriched: {enriched_count} concepts")
    print(f"  Low confidence (<0.6): {len(low_confidence)}")
    for nid, label, conf, ch in low_confidence:
        print(f"    [{nid}] {label}: {conf} (ch: {ch})")

    # TASK C: Create refresh-state.json
    chapter_state = {}
    for ch_dir in chapter_dirs:
        ch_index = ch_dir / "index.md"
        if not ch_index.exists():
            continue
        ch_name = ch_dir.name
        chapter_state[ch_name] = {
            "content_hash": compute_file_hash(ch_index),
            "word_count": count_words(ch_index),
            "has_concept_markers": bool(re.search(
                r'<!-- concept:\d+ -->', open(ch_index).read())),
        }

    lg_hash = compute_file_hash(lg_path)

    faq_path = textbook_dir / "docs" / "faq.md"
    faq_state = None
    if faq_path.exists():
        faq_state = {
            "content_hash": compute_file_hash(faq_path),
            "word_count": count_words(faq_path),
        }

    refresh_state = {
        "schema_version": "1.0",
        "baseline": {
            "source_commit": source_commit,
            "refresh_date": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "tool_versions": {
                "python": "3.12",
                "enrich_script": "1.0.0",
            }
        },
        "chapter_state": chapter_state,
        "learning_graph_state": {
            "content_hash": lg_hash,
            "node_count": len(lg_data["nodes"]),
            "edge_count": len(lg_data["edges"]),
            "enriched_nodes": enriched_count,
            "low_confidence_count": len(low_confidence),
        },
        "faq_state": faq_state,
        "refresh_history": [],
    }

    refresh_path = textbook_dir / "refresh-state.json"
    save_json(refresh_path, refresh_state)

    print(f"\n  SUMMARY for {name}:")
    print(f"    Concepts enriched: {enriched_count}")
    print(f"    Markers placed: {total_markers_placed}")
    print(f"    Markers failed: {total_markers_failed}")
    print(f"    Low-confidence matches: {len(low_confidence)}")
    if all_marker_results:
        print(f"    Unplaced markers:")
        for ch, cid, clabel in all_marker_results:
            print(f"      [{cid}] {clabel} in {ch}")

    return {
        "name": name,
        "enriched": enriched_count,
        "markers_placed": total_markers_placed,
        "markers_failed": total_markers_failed,
        "low_confidence": low_confidence,
        "unplaced": all_marker_results,
    }


if __name__ == "__main__":
    results = []
    for proj in PROJECTS:
        r = process_project(proj)
        results.append(r)

    print("\n" + "=" * 60)
    print("FINAL SUMMARY")
    print("=" * 60)
    for r in results:
        print(f"\n{r['name']}:")
        print(f"  Enriched: {r['enriched']} concepts")
        print(f"  Markers placed: {r['markers_placed']}, failed: {r['markers_failed']}")
        print(f"  Low-confidence: {len(r['low_confidence'])}")

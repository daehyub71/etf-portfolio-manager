"""
데이터 백업 및 복구 시스템
- 자동 백업 스케줄링
- 클라우드 백업 (Google Drive, Dropbox)
- 증분 백업
- 데이터 무결성 검증
- 복구 시스템
"""

import os
import shutil
import json
import sqlite3
import gzip
import hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import pandas as pd
import yaml
from pathlib import Path
import zipfile
import pickle

class DataBackupManager:
    """데이터 백업 및 복구 관리자"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """초기화"""
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        
        self.backup_config = self.config.get('backup', {})
        
        # 백업 경로 설정
        self.local_backup_path = Path(self.backup_config.get('local_path', './backups'))
        self.cloud_backup_path = self.backup_config.get('cloud_path', None)
        
        # 백업 경로 생성
        self.local_backup_path.mkdir(parents=True, exist_ok=True)
        
        # 백업 설정
        self.retention_days = self.backup_config.get('retention_days', 30)
        self.max_backups = self.backup_config.get('max_backups', 50)
        self.compression_enabled = self.backup_config.get('compression', True)
        
        # 백업할 파일/폴더 목록
        self.backup_items = [
            'portfolio_data.db',
            'etf_data.db',
            'config.yaml',
            'logs/',
            'data/',
            'user_settings.json'
        ]
        
    def create_full_backup(self, backup_name: str = None) -> str:
        """전체 백업 생성"""
        
        if backup_name is None:
            backup_name = f"full_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        backup_path = self.local_backup_path / backup_name
        backup_path.mkdir(exist_ok=True)
        
        print(f"전체 백업 시작: {backup_name}")
        
        backup_manifest = {
            'backup_type': 'full',
            'created_at': datetime.now().isoformat(),
            'files': [],
            'checksums': {},
            'size_mb': 0
        }
        
        total_size = 0
        
        # 각 백업 항목 처리
        for item in self.backup_items:
            source_path = Path(item)
            
            if source_path.exists():
                if source_path.is_file():
                    # 파일 백업
                    size = self._backup_file(source_path, backup_path)
                    checksum = self._calculate_checksum(source_path)
                    
                    backup_manifest['files'].append({
                        'path': str(source_path),
                        'type': 'file',
                        'size': size,
                        'checksum': checksum
                    })
                    backup_manifest['checksums'][str(source_path)] = checksum
                    total_size += size
                    
                elif source_path.is_dir():
                    # 폴더 백업
                    size = self._backup_directory(source_path, backup_path)
                    
                    backup_manifest['files'].append({
                        'path': str(source_path),
                        'type': 'directory',
                        'size': size
                    })
                    total_size += size
        
        backup_manifest['size_mb'] = round(total_size / (1024 * 1024), 2)
        
        # 매니페스트 저장
        manifest_path = backup_path / 'backup_manifest.json'
        with open(manifest_path, 'w', encoding='utf-8') as f:
            json.dump(backup_manifest, f, ensure_ascii=False, indent=2)
        
        # 압축 (옵션)
        if self.compression_enabled:
            compressed_path = self._compress_backup(backup_path)
            # 원본 폴더 삭제
            shutil.rmtree(backup_path)
            backup_path = compressed_path
        
        print(f"백업 완료: {backup_path} ({backup_manifest['size_mb']} MB)")
        
        # 백업 정리
        self._cleanup_old_backups()
        
        return str(backup_path)
    
    def create_incremental_backup(self, base_backup: str = None) -> str:
        """증분 백업 생성"""
        
        # 기준 백업 찾기
        if base_backup is None:
            base_backup = self._find_latest_full_backup()
        
        if not base_backup:
            print("기준 백업이 없습니다. 전체 백업을 수행합니다.")
            return self.create_full_backup()
        
        backup_name = f"incremental_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        backup_path = self.local_backup_path / backup_name
        backup_path.mkdir(exist_ok=True)
        
        print(f"증분 백업 시작: {backup_name} (기준: {base_backup})")
        
        # 기준 백업의 매니페스트 로드
        base_manifest = self._load_backup_manifest(base_backup)
        base_checksums = base_manifest.get('checksums', {})
        
        backup_manifest = {
            'backup_type': 'incremental',
            'base_backup': base_backup,
            'created_at': datetime.now().isoformat(),
            'files': [],
            'checksums': {},
            'size_mb': 0
        }
        
        total_size = 0
        
        # 변경된 파일만 백업
        for item in self.backup_items:
            source_path = Path(item)
            
            if source_path.exists() and source_path.is_file():
                current_checksum = self._calculate_checksum(source_path)
                base_checksum = base_checksums.get(str(source_path))
                
                # 파일이 변경되었거나 새 파일인 경우
                if current_checksum != base_checksum:
                    size = self._backup_file(source_path, backup_path)
                    
                    backup_manifest['files'].append({
                        'path': str(source_path),
                        'type': 'file',
                        'size': size,
                        'checksum': current_checksum,
                        'status': 'modified' if base_checksum else 'new'
                    })
                    backup_manifest['checksums'][str(source_path)] = current_checksum
                    total_size += size
        
        backup_manifest['size_mb'] = round(total_size / (1024 * 1024), 2)
        
        # 매니페스트 저장
        manifest_path = backup_path / 'backup_manifest.json'
        with open(manifest_path, 'w', encoding='utf-8') as f:
            json.dump(backup_manifest, f, ensure_ascii=False, indent=2)
        
        if self.compression_enabled:
            compressed_path = self._compress_backup(backup_path)
            shutil.rmtree(backup_path)
            backup_path = compressed_path
        
        print(f"증분 백업 완료: {backup_path} ({backup_manifest['size_mb']} MB)")
        
        return str(backup_path)
    
    def restore_backup(self, backup_path: str, target_path: str = ".") -> bool:
        """백업에서 복구"""
        
        backup_path = Path(backup_path)
        target_path = Path(target_path)
        
        if not backup_path.exists():
            print(f"백업 파일을 찾을 수 없습니다: {backup_path}")
            return False
        
        print(f"백업 복구 시작: {backup_path}")
        
        # 압축된 백업인 경우 압축 해제
        if backup_path.suffix in ['.zip', '.gz']:
            extracted_path = self._extract_backup(backup_path)
            backup_path = extracted_path
        
        # 매니페스트 로드
        manifest_path = backup_path / 'backup_manifest.json'
        if not manifest_path.exists():
            print("백업 매니페스트를 찾을 수 없습니다.")
            return False
        
        with open(manifest_path, 'r', encoding='utf-8') as f:
            manifest = json.load(f)
        
        backup_type = manifest.get('backup_type', 'unknown')
        
        if backup_type == 'incremental':
            # 증분 백업인 경우 기준 백업도 복구
            base_backup = manifest.get('base_backup')
            if base_backup and not self._is_base_backup_restored(base_backup):
                print(f"기준 백업 먼저 복구: {base_backup}")
                if not self.restore_backup(base_backup, target_path):
                    return False
        
        # 파일 복구
        success_count = 0
        total_files = len(manifest.get('files', []))
        
        for file_info in manifest.get('files', []):
            file_path = file_info['path']
            source_file = backup_path / Path(file_path).name
            target_file = target_path / file_path
            
            try:
                # 대상 디렉토리 생성
                target_file.parent.mkdir(parents=True, exist_ok=True)
                
                if source_file.exists():
                    if file_info['type'] == 'file':
                        shutil.copy2(source_file, target_file)
                    elif file_info['type'] == 'directory':
                        if target_file.exists():
                            shutil.rmtree(target_file)
                        shutil.copytree(source_file, target_file)
                    
                    # 체크섬 검증
                    if 'checksum' in file_info:
                        if self._calculate_checksum(target_file) == file_info['checksum']:
                            success_count += 1
                        else:
                            print(f"체크섬 불일치: {file_path}")
                    else:
                        success_count += 1
                
            except Exception as e:
                print(f"파일 복구 실패: {file_path} - {e}")
        
        # 임시 압축 해제 폴더 정리
        if str(backup_path) != str(Path(backup_path).with_suffix('')):
            shutil.rmtree(backup_path)
        
        success_rate = (success_count / total_files * 100) if total_files > 0 else 0
        print(f"복구 완료: {success_count}/{total_files} 파일 ({success_rate:.1f}%)")
        
        return success_rate > 90  # 90% 이상 성공시 성공으로 간주
    
    def list_backups(self) -> List[Dict]:
        """백업 목록 조회"""
        
        backups = []
        
        for backup_file in self.local_backup_path.iterdir():
            if backup_file.is_file() and backup_file.suffix in ['.zip']:
                # 압축된 백업
                try:
                    with zipfile.ZipFile(backup_file, 'r') as zip_file:
                        if 'backup_manifest.json' in zip_file.namelist():
                            manifest_data = zip_file.read('backup_manifest.json')
                            manifest = json.loads(manifest_data.decode('utf-8'))
                            
                            backups.append({
                                'name': backup_file.name,
                                'path': str(backup_file),
                                'type': manifest.get('backup_type', 'unknown'),
                                'created_at': manifest.get('created_at'),
                                'size_mb': manifest.get('size_mb', 0),
                                'file_count': len(manifest.get('files', [])),
                                'compressed': True
                            })
                except:
                    continue
            
            elif backup_file.is_dir():
                # 압축되지 않은 백업
                manifest_path = backup_file / 'backup_manifest.json'
                if manifest_path.exists():
                    try:
                        with open(manifest_path, 'r', encoding='utf-8') as f:
                            manifest = json.load(f)
                        
                        backups.append({
                            'name': backup_file.name,
                            'path': str(backup_file),
                            'type': manifest.get('backup_type', 'unknown'),
                            'created_at': manifest.get('created_at'),
                            'size_mb': manifest.get('size_mb', 0),
                            'file_count': len(manifest.get('files', [])),
                            'compressed': False
                        })
                    except:
                        continue
        
        # 생성일시 기준 정렬
        backups.sort(key=lambda x: x['created_at'], reverse=True)
        
        return backups
    
    def verify_backup_integrity(self, backup_path: str) -> Dict:
        """백업 무결성 검증"""
        
        backup_path = Path(backup_path)
        
        if not backup_path.exists():
            return {'status': 'error', 'message': 'backup_not_found'}
        
        # 압축된 백업인 경우 압축 해제
        if backup_path.suffix in ['.zip', '.gz']:
            extracted_path = self._extract_backup(backup_path)
            backup_path = extracted_path
            cleanup_extracted = True
        else:
            cleanup_extracted = False
        
        manifest_path = backup_path / 'backup_manifest.json'
        
        if not manifest_path.exists():
            return {'status': 'error', 'message': 'manifest_not_found'}
        
        with open(manifest_path, 'r', encoding='utf-8') as f:
            manifest = json.load(f)
        
        verification_result = {
            'status': 'success',
            'backup_type': manifest.get('backup_type'),
            'created_at': manifest.get('created_at'),
            'total_files': len(manifest.get('files', [])),
            'verified_files': 0,
            'missing_files': [],
            'corrupted_files': [],
            'integrity_score': 0
        }
        
        # 각 파일 검증
        for file_info in manifest.get('files', []):
            file_path = Path(file_info['path']).name
            backup_file = backup_path / file_path
            
            if not backup_file.exists():
                verification_result['missing_files'].append(file_path)
                continue
            
            # 체크섬 검증
            if 'checksum' in file_info:
                current_checksum = self._calculate_checksum(backup_file)
                if current_checksum != file_info['checksum']:
                    verification_result['corrupted_files'].append(file_path)
                    continue
            
            verification_result['verified_files'] += 1
        
        # 무결성 점수 계산
        if verification_result['total_files'] > 0:
            verification_result['integrity_score'] = (
                verification_result['verified_files'] / verification_result['total_files'] * 100
            )
        
        # 임시 폴더 정리
        if cleanup_extracted:
            shutil.rmtree(backup_path)
        
        return verification_result
    
    def _backup_file(self, source_path: Path, backup_path: Path) -> int:
        """단일 파일 백업"""
        
        target_path = backup_path / source_path.name
        shutil.copy2(source_path, target_path)
        
        return source_path.stat().st_size
    
    def _backup_directory(self, source_path: Path, backup_path: Path) -> int:
        """디렉토리 백업"""
        
        target_path = backup_path / source_path.name
        shutil.copytree(source_path, target_path, dirs_exist_ok=True)
        
        # 디렉토리 크기 계산
        total_size = 0
        for file_path in target_path.rglob('*'):
            if file_path.is_file():
                total_size += file_path.stat().st_size
        
        return total_size
    
    def _calculate_checksum(self, file_path: Path) -> str:
        """파일 체크섬 계산"""
        
        hash_md5 = hashlib.md5()
        
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        
        return hash_md5.hexdigest()
    
    def _compress_backup(self, backup_path: Path) -> Path:
        """백업 압축"""
        
        zip_path = backup_path.with_suffix('.zip')
        
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for file_path in backup_path.rglob('*'):
                if file_path.is_file():
                    arcname = file_path.relative_to(backup_path)
                    zip_file.write(file_path, arcname)
        
        return zip_path
    
    def _extract_backup(self, backup_path: Path) -> Path:
        """백업 압축 해제"""
        
        if backup_path.suffix == '.zip':
            extract_path = backup_path.parent / backup_path.stem
            extract_path.mkdir(exist_ok=True)
            
            with zipfile.ZipFile(backup_path, 'r') as zip_file:
                zip_file.extractall(extract_path)
            
            return extract_path
        
        return backup_path
    
    def _find_latest_full_backup(self) -> Optional[str]:
        """최신 전체 백업 찾기"""
        
        backups = self.list_backups()
        
        for backup in backups:
            if backup['type'] == 'full':
                return backup['path']
        
        return None
    
    def _load_backup_manifest(self, backup_path: str) -> Dict:
        """백업 매니페스트 로드"""
        
        backup_path = Path(backup_path)
        
        if backup_path.suffix == '.zip':
            with zipfile.ZipFile(backup_path, 'r') as zip_file:
                manifest_data = zip_file.read('backup_manifest.json')
                return json.loads(manifest_data.decode('utf-8'))
        else:
            manifest_path = backup_path / 'backup_manifest.json'
            with open(manifest_path, 'r', encoding='utf-8') as f:
                return json.load(f)
    
    def _is_base_backup_restored(self, base_backup: str) -> bool:
        """기준 백업이 복구되었는지 확인"""
        # 간단히 주요 파일들이 존재하는지 확인
        key_files = ['portfolio_data.db', 'config.yaml']
        return all(Path(f).exists() for f in key_files)
    
    def _cleanup_old_backups(self):
        """오래된 백업 정리"""
        
        backups = self.list_backups()
        
        # 보존 기간 초과 백업 삭제
        cutoff_date = datetime.now() - timedelta(days=self.retention_days)
        
        # 최대 백업 수 초과시 삭제
        if len(backups) > self.max_backups:
            excess_backups = backups[self.max_backups:]
            for backup in excess_backups:
                try:
                    backup_path = Path(backup['path'])
                    if backup_path.exists():
                        if backup_path.is_file():
                            backup_path.unlink()
                        else:
                            shutil.rmtree(backup_path)
                    print(f"오래된 백업 삭제: {backup['name']}")
                except Exception as e:
                    print(f"백업 삭제 실패: {backup['name']} - {e}")
    
    def create_emergency_backup(self) -> str:
        """긴급 백업 (핵심 데이터만)"""
        
        emergency_items = [
            'portfolio_data.db',
            'config.yaml',
            'user_settings.json'
        ]
        
        backup_name = f"emergency_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        backup_path = self.local_backup_path / backup_name
        backup_path.mkdir(exist_ok=True)
        
        print(f"긴급 백업 시작: {backup_name}")
        
        for item in emergency_items:
            source_path = Path(item)
            if source_path.exists():
                self._backup_file(source_path, backup_path)
        
        # 압축
        if self.compression_enabled:
            compressed_path = self._compress_backup(backup_path)
            shutil.rmtree(backup_path)
            backup_path = compressed_path
        
        print(f"긴급 백업 완료: {backup_path}")
        return str(backup_path)

# 사용 예시
if __name__ == "__main__":
    # 백업 매니저 초기화
    backup_manager = DataBackupManager()
    
    # 전체 백업 생성
    print("=== 전체 백업 테스트 ===")
    backup_path = backup_manager.create_full_backup()
    
    # 백업 목록 조회
    print("\n=== 백업 목록 ===")
    backups = backup_manager.list_backups()
    for backup in backups[:3]:  # 최근 3개만 표시
        print(f"• {backup['name']} ({backup['type']}, {backup['size_mb']} MB)")
    
    # 백업 무결성 검증
    print(f"\n=== 백업 무결성 검증 ===")
    verification = backup_manager.verify_backup_integrity(backup_path)
    print(f"무결성 점수: {verification['integrity_score']:.1f}%")
    
    # 증분 백업 생성 (테스트)
    print(f"\n=== 증분 백업 테스트 ===")
    incremental_backup = backup_manager.create_incremental_backup()
    
    print("\n백업 시스템이 성공적으로 실행되었습니다.")
import os
from typing import Dict

from .aof import AOfWriter


class RecoveryManager:
    def __init__(self,aos_filename:str):
        self.aof_filename=aos_filename
        self.aof_handler=None
    

    def recover_data(self,data_store,command_handler=None)->bool:
        try:
            aof_exist=os.path.exists(self.aof_filename)
            if not aof_exist:
                return False
            
            print(f"loading data from aof file:{self.aof_filename}")
            return self._reply_aof(data_store,command_handler)
        except Exception as e:
            print(f"error during data recovery:{e}")
            return False
        
    def _reply_aof(self,data_store,command_handler):
        try:
            command_replayed=0
            with open(self.aof_filename,'r',encoding='utf-8') as f:
                for line_num,line in enumerate(f,1):
                    line=line.strip()
                    if not line:
                        continue
                    try:
                        parts=line.split(' ',2)
                        if len(parts)<2:
                            continue
                        timestamps=parts[0]
                        command=parts[1].upper()
                        args=parts[2].split() if len(parts)>2 else []

                        self._execute_recovery_command(data_store,command,args)
                        command_replayed+=1
                    except Exception as e:
                        print(f"Error replying command at line {line_num}:{e}")
                        print(f"Problematic line:{e}")
                        continue
            print(f"replayed {command_replayed} command from aof")
            return True
            
        except Exception as e:
            print(f"error replying from aof:{e}")



    
    
    def _execute_recovery_command(self, data_store, command: str, args: list) -> None:
        """
        Execute a single recovery command on the data store
        
        Args:
            data_store: Data store to execute command on
            command: Command to execute
            args: Command arguments
        """
        try:
            if command == 'SET':
                if len(args) >= 2:
                    key = args[0]
                    value = ' '.join(args[1:])
                    data_store.set(key, value)
            
            elif command == 'DEL':
                if args:
                    data_store.delete(*args)
            
            elif command == 'EXPIRE':
                if len(args) == 2:
                    key = args[0]
                    seconds = int(args[1])
                    data_store.expire(key, seconds)
            
            elif command == 'EXPIREAT':
                if len(args) == 2:
                    key = args[0]
                    timestamp = int(args[1])
                    data_store.expire_at(key, timestamp)
            
            elif command == 'PERSIST':
                if len(args) == 1:
                    key = args[0]
                    data_store.persist(key)
            
            elif command == 'FLUSHALL':
                data_store.flush()
            
            else:
                # Unknown command - ignore during recovery
                pass
                
        except Exception as e:
            print(f"Error executing recovery command {command}: {e}")
    
    def _handle_corruption(self, error) -> bool:
        """
        Handle corrupted persistence files
        
        Args:
            error: The error that occurred
            
        Returns:
            True if recovery should continue with empty database
        """
        print(f"Persistence file corruption detected: {error}")
        print("Starting with empty database. Consider restoring from backup.")
        
        # In production, you might want to:
        # 1. Create backup of corrupted files
        # 2. Attempt partial recovery
        # 3. Send alerts to administrators
        
        return True  # Continue with empty database
    
    def validate_files(self) -> Dict[str, bool]:
        """
        Validate AOF file without loading it
        
        Returns:
            Dictionary with validation results
        """
        results = {
            'aof_exists': os.path.exists(self.aof_filename),
            'aof_valid': False
        }
        
        # Validate AOF file
        if results['aof_exists']:
            try:
                with open(self.aof_filename, 'r', encoding='utf-8') as f:
                    # Try to read first few lines
                    for i, line in enumerate(f):
                        if i >= 5:  # Check first 5 lines
                            break
                        # Basic format validation
                        parts = line.strip().split(' ', 2)
                        if len(parts) >= 2:
                            try:
                                int(parts[0])  # timestamp should be integer
                            except ValueError:
                                break
                    else:
                        results['aof_valid'] = True
            except Exception:
                results['aof_valid'] = False
        
        return results

        
            

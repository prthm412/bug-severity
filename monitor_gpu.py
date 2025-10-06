"""
Simple GPU monitor for training sessions
"""
import subprocess
import re
import time

def get_gpu_stats():
    """Get GPU temperature, memory, and load"""
    try:
        # Try nvidia-smi with specific query
        result = subprocess.run(
            ['nvidia-smi', '--query-gpu=temperature.gpu,memory.used,memory.total,utilization.gpu', 
             '--format=csv,noheader,nounits'],
            capture_output=True,
            text=True,
            timeout=5
        )
        
        if result.returncode == 0:
            temp, mem_used, mem_total, load = result.stdout.strip().split(', ')
            return {
                'temp': int(temp),
                'mem_used': int(mem_used),
                'mem_total': int(mem_total),
                'load': int(load)
            }
    except:
        pass
    
    return None

def monitor_loop(interval=5, duration=60):
    """Monitor GPU for specified duration"""
    print("🔍 GPU Monitor - Press Ctrl+C to stop\n")
    print(f"{'Time':<10} {'Temp':<10} {'Memory':<20} {'Load':<10}")
    print("-" * 60)
    
    start_time = time.time()
    
    try:
        while True:
            stats = get_gpu_stats()
            
            if stats:
                elapsed = int(time.time() - start_time)
                temp_emoji = "🔥" if stats['temp'] > 75 else "✅"
                
                print(f"{elapsed}s{' '*(8-len(str(elapsed)))} "
                      f"{temp_emoji} {stats['temp']}°C{' '*(5-len(str(stats['temp'])))} "
                      f"{stats['mem_used']}MB / {stats['mem_total']}MB{' '*(8-len(str(stats['mem_used'])))} "
                      f"{stats['load']}%")
                
                # Warning if too hot
                if stats['temp'] > 80:
                    print("⚠️  WARNING: GPU temperature high! Consider reducing batch size.")
            else:
                print("⚠️  Could not read GPU stats")
            
            time.sleep(interval)
            
            if duration and (time.time() - start_time) > duration:
                break
                
    except KeyboardInterrupt:
        print("\n\n✅ Monitoring stopped")

if __name__ == "__main__":
    monitor_loop(interval=2)  # Check every 2 seconds
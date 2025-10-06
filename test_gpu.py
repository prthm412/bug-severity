"""
Test GPU availability and monitor temperature
"""
import torch
import sys

def test_gpu():
    print("=" * 60)
    print("🔍 BugSage+ GPU Test")
    print("=" * 60)
    
    # Check PyTorch
    print(f"\n✅ PyTorch version: {torch.__version__}")
    
    # Check CUDA
    if torch.cuda.is_available():
        print(f"✅ CUDA available: Yes")
        print(f"✅ CUDA version: {torch.version.cuda}")
        print(f"✅ GPU count: {torch.cuda.device_count()}")
        
        # GPU details
        for i in range(torch.cuda.device_count()):
            print(f"\n🎮 GPU {i}: {torch.cuda.get_device_name(i)}")
            
            # Memory
            total_memory = torch.cuda.get_device_properties(i).total_memory / 1e9
            print(f"   Total memory: {total_memory:.2f} GB")
            
            # Test computation
            try:
                x = torch.randn(1000, 1000).cuda(i)
                y = torch.matmul(x, x)
                print(f"   ✅ Computation test: PASSED")
            except Exception as e:
                print(f"   ❌ Computation test: FAILED - {e}")
        
        # Temperature check (if GPUtil available)
        try:
            import GPUtil
            gpus = GPUtil.getGPUs()
            if gpus:
                gpu = gpus[0]
                print(f"\n🌡️  Temperature: {gpu.temperature}°C")
                print(f"📊 Memory used: {gpu.memoryUsed}MB / {gpu.memoryTotal}MB")
                print(f"⚡ Load: {gpu.load * 100:.1f}%")
                
                if gpu.temperature > 75:
                    print("⚠️  WARNING: GPU is hot! Consider reducing batch size.")
                else:
                    print("✅ Temperature is good!")
        except ImportError:
            print("\nℹ️  Install GPUtil for temperature monitoring: pip install gputil")
        
        print("\n" + "=" * 60)
        print("🎉 GPU Setup Complete!")
        print("💡 Recommended settings for RTX 4060:")
        print("   - Batch size: 4-8")
        print("   - Mixed precision: True (FP16)")
        print("   - Gradient accumulation: 4")
        print("=" * 60)
        
    else:
        print("❌ CUDA not available")
        print("⚠️  PyTorch will use CPU (much slower)")
        print("\n🔧 To fix:")
        print("1. Check NVIDIA drivers are installed")
        print("2. Reinstall PyTorch with CUDA:")
        print("   pip install torch --index-url https://download.pytorch.org/whl/cu121")
        
        sys.exit(1)

if __name__ == "__main__":
    test_gpu()
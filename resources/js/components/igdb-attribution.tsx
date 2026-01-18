import React from 'react';

export default function IgdbAttribution({ className = '' }: { className?: string }) {
    return (
        <a 
            href="https://www.igdb.com" 
            target="_blank" 
            rel="noopener noreferrer" 
            className={`inline-flex items-center gap-2 opacity-75 hover:opacity-100 transition-opacity ${className}`}
            title="Data provided by IGDB"
        >
            <span className="text-[10px] font-bold uppercase tracking-widest text-muted-foreground">Powered by</span>
            <div className="flex items-center font-black text-xl tracking-tighter leading-none">
                <span className="text-[#9147ff]">IG</span>
                <span className="text-current">DB</span>
            </div>
        </a>
    );
}
